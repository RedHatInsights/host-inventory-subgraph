import {buildFederatedSchema} from '@apollo/federation';
import {ApolloError, ApolloServer, gql, UserInputError} from 'apollo-server-express';
import express from 'express';
import config from 'config';
import Logger from "./logging/logger";
import morganMiddleware from "./middleware/morgan";
import got from "got";
import {Client} from "@elastic/elasticsearch";
import * as _ from 'lodash';

async function start() {
    const app = express();

    const typeDefs = gql`
        type Query {
            hostTags (
                #                hostFilter: HostFilter, //TODO
                filter: TagAggregationFilter,
                limit: Int = 10,
                offset: Int = 0,
                order_by: HOST_TAGS_ORDER_BY = count,
                order_how: ORDER_DIR = DESC
            ): HostTags
        }

        enum ORDER_DIR {
            ASC,
            DESC
        }

        enum HOST_TAGS_ORDER_BY {
            tag,
            count
        }

        input TagAggregationFilter {
            """
            Limits the aggregation to tags that match the given search term.
            The search term is a regular exression that operates on a string representation of a tag.
            The string representation has a form of "namespace/key=value" i.e. the segments are concatenated together using "=" and "/", respectively.
            There is no expecing of the control characters in the segments.
            As a result, "=" and "/" appear in every tag.
            """
            search: FilterStringWithRegex
        }

        """
        String field filter that allows filtering based on exact match or using regular expression.
        """
        input FilterStringWithRegex {
            """
            Compares the document field with the provided value.
            If \`null\` is provided then documents where the given field does not exist are returned.
            """
            eq: String

            """
            Matches the document field against the provided regular expression.
            """
            regex: String
        }

        type HostTags {
            data: [TagInfo]!
            meta: CollectionMeta!
        }

        type CollectionMeta {
            "number of returned results"
            count: Int!
            "total number of entities matching the query"
            total: Int!
        }

        type StructuredTag {
            namespace: String,
            key: String!,
            value: String
        }

        type Tags {
            data: [StructuredTag]!
            meta: CollectionMeta!
        }

        type TagInfo {
            tag: StructuredTag!
            count: Int!
        }
    `;

    await register(typeDefs.toString());

    const resolvers = {
        Query: {
            hostTags: hostTagsResolver
        }
    };

    const server = new ApolloServer({
        schema: buildFederatedSchema({typeDefs, resolvers}),
    });

    await server.start();

    const router = express.Router();
    app.use(router);
    app.use(morganMiddleware);
    server.applyMiddleware({app});

    app.listen({port: config.get("Port")}, () => {
        Logger.info(`Server ready at http://localhost:${config.get("Port")}/graphql`);
    });
}

async function register(schema: string) {
    const sr = {
        protocol: config.get("SchemaRegistry.Protocol"),
        hostname: config.get("SchemaRegistry.Hostname"),
        port: config.get("SchemaRegistry.Port")
    }
    const baseUrl = `${sr.protocol}://${sr.hostname}:${sr.port}`;
    let artifactId = `xjoinindexpipeline.inventory.hosts.tags.test`;

    try {
        await got.get(`${baseUrl}/apis/registry/v2/groups/default/artifacts/${artifactId}`);

        await got.post(
            `${sr.protocol}://${sr.hostname}:${sr.port}/apis/registry/v2/groups/default/artifacts/${artifactId}/versions`,
            {
                body: schema,
                headers: {
                    'Content-Type': 'application/graphql',
                    'X-Registry-ArtifactType': 'GRAPHQL'
                }
            });
    } catch (e) {
        Logger.error('Unable to create graphql schema');
        throw e;
    }
}

export function checkMin(min: number, value: number | null | undefined): void {
    if (value === null || value === undefined) {
        return;
    }

    if (value < min) {
        throw new UserInputError(`value must be ${min} or greater (was ${value})`);
    }
}

export function checkMax(max: number, value: number | null | undefined): void {
    if (value === null || value === undefined) {
        return;
    }

    if (value > max) {
        throw new UserInputError(`value must be ${max} or less (was ${value})`);
    }
}

export function checkLimit(limit: number | null | undefined): void {
    checkMin(0, limit);
    checkMax(100, limit);
}

export function checkOffset(offset: number | null | undefined): void {
    checkMin(0, offset);
}

export function defaultValue(value: number | undefined | null, def: number): number {
    if (value === undefined || value === null) {
        return def;
    }

    return value;
}

export function extractPage(list: any, limit: number, offset: number): any {
    return list.slice(offset, offset + limit);
}

const TAG_ORDER_BY_MAPPING: { [key: string]: string } = {
    count: '_count',
    tag: '_key'
};

export async function runQuery(query: any, id: string): Promise<any> {
    // log.trace(query, 'executing query');

    const client = new Client({
        node: config.get('ElasticSearch.URL'),
        auth: {
            username: config.get('ElasticSearch.Username'),
            password: config.get('ElasticSearch.Password')
        }
    });

    try {
        return await client.search(query);
        // log.trace(result, 'query finished');
        // esResponseHistogram.labels(id).observe(result.body.took / 1000); // ms -> seconds
    } catch (err) {
        console.error(err);

        if (_.get(err, 'meta.body.error.root_cause[0].reason', '').startsWith('Result window is too large')) {
            // check if the request should have succeeded (eg. the requested page
            // contains hosts that should be able to be queried)
            const requestedHostNumber = query.body.from;

            query.body.from = 0;
            query.body.size = 0;

            const countQueryRes = await client.search(query);

            const hits = countQueryRes.body.hits.total.value;

            // only return the request window error if the requested page should
            // have contained at least one host
            if (hits >= requestedHostNumber) {
                throw new ResultWindowError(err);
            }

            // return an empty response (same behavior as when there is not host
            // at the specified offset within result window)
            return countQueryRes;
        }

        throw new ElasticSearchError(err);
    }
}

export class ElasticSearchError extends ApolloError {
    constructor(originalError: any, message = 'Elastic search error', code = 'ELASTIC_SEARCH_ERROR') {
        super(message, code, {originalError});
    }
}

export class ResultWindowError extends ElasticSearchError {
    constructor(originalError: any,
                message = 'Request could not be completed because the page is too deep',
                code = 'REQUEST_WINDOW_ERROR')
    {
        super(originalError, message, code);
    }
}

export function buildFilterQuery(filter: any | null | undefined, account_number: string): any {
    return {
        bool: {
            filter: [
                {term: {'host.account': 'test'}}, // implicit filter based on x-rh-identity
                // ...(filter ? resolveFilter(filter) : [])
            ]
        }
    };
}

async function hostTagsResolver(parent: any, args: any, context: any): Promise<Record<string, unknown>> {
    checkLimit(args.limit);
    checkOffset(args.offset);

    const limit = defaultValue(args.limit, 10);
    const offset = defaultValue(args.offset, 0);

    const body: any = {
        _source: [],
        query: buildFilterQuery(args.hostFilter, context.account_number), //TODO
        size: 0,
        aggs: {
            tags: {
                terms: {
                    field: 'host.tags_search',
                    size: 10000, //TODO
                    order: [{
                        [TAG_ORDER_BY_MAPPING[String(args.order_by)]]: String(args.order_how)
                    }, {
                        _key: 'ASC' // for deterministic sort order
                    }],
                    show_term_doc_count_error: true
                }
            }
        }
    };

    if (args.filter && args.filter.search) {
        const search = args.filter.search;
        if (search.eq) {
            body.aggs.tags.terms.include = [search.eq];
        } else if (search.regex) {
            body.aggs.tags.terms.include = search.regex;
        }
    }

    const result = await runQuery({
        index: config.get('ElasticSearch.Index'),
        body
    }, 'hostTags');

    const page = extractPage(
        result.body.aggregations.tags.buckets,
        limit,
        offset
    );

    const data = _.map(page, bucket => {

        function split(value: string, delimiter: string) {
            const index = value.indexOf(delimiter);

            if (index === -1) {
                throw new Error(`cannot split ${value} using ${delimiter}`);
            }

            return [value.substring(0, index), value.substring(index + 1)];
        }

        function normalizeTag(value: string, key: string) {
            if (value === '' && key !== 'key') {
                return null;
            }

            return value;
        }

        // This assumes that a namespace never contains '/'
        // We control the namespaces so this should be a safe assumption;
        const [namespace, rest] = split(bucket.key, '/');

        // This assumes that the key ends with the first '='
        // That may not be accurate in a situation when someone defines a key that contains '='
        // This should rarely happen but if it does we can solve that by issuing another ES query to clear up the ambiguity
        const [key, value] = split(rest, '=');

        const tag = _.mapValues({
            namespace,
            key,
            value
        }, normalizeTag);

        return {
            tag,
            count: bucket.doc_count
        };
    });

    return {
        data,
        meta: {
            count: data.length,
            total: result.body.aggregations.tags.buckets.length
        }
    };


    /*
    return {
        data: [{
            tag: {
                namespace: 'ns',
                key: 'key',
                value: 'val'
            },
            count: 1
        }],
        meta: {
            count: 1,
            total: 1
        }
    };
     */
}

start();
