import {buildFederatedSchema} from '@apollo/federation';
import {ApolloServer, gql} from 'apollo-server-express';
import express from 'express';
import config from 'config';
import {Client} from "@elastic/elasticsearch";
import { readFileSync } from 'fs';
import {
    SchemaRegistry,
    morganMiddleware,
    Logger,
    ElasticSearchError,
    ResultWindowError,
    checkLimit, checkOffset, defaultValue, extractPage
} from "xjoin-subgraph-utils";

async function start() {
    const app = express();

    //load the GraphQL schema with custom queries
    const graphqlSchema = readFileSync(new URL('./schema.graphql', import.meta.url).pathname, 'utf-8');

    //register the GraphQL schema with the ApiCurio schema registry
    const sr = new SchemaRegistry({
        protocol: config.get('SchemaRegistry.Protocol'),
        hostname: config.get('SchemaRegistry.Hostname'),
        port: config.get('SchemaRegistry.Port')
    });
    await sr.registerGraphQLSchema(config.get('GraphQLSchemaName'), graphqlSchema)

    const resolvers = {
        Query: {
            HostTags: hostTagsResolver
        }
    };

    //start the Apollo GraphQL server
    const server = new ApolloServer({
        schema: buildFederatedSchema({
            typeDefs: gql(graphqlSchema),
            resolvers: resolvers
        }),
    });
    await server.start();

    //add a middleware to log each request
    const router = express.Router();
    app.use(router);
    app.use(morganMiddleware);
    server.applyMiddleware({app});

    app.listen({port: config.get('Port')}, () => {
        Logger.info(`Server ready at http://localhost:${config.get("Port")}/graphql`);
    });
}

async function hostTagsResolver(parent: any, args: any, context: any): Promise<Record<string, unknown>> {
    checkLimit(args.limit);
    checkOffset(args.offset);

    const limit = defaultValue(args.limit, 10);
    const offset = defaultValue(args.offset, 0);

    //buildFilterQuery(args.hostFilter, context.account_number), //TODO
    const body: any = {
        _source: [],
        query: {
            bool: {
                filter: [
                    {term: {'host.account': 'test'}}, // implicit filter based on x-rh-identity
                    // ...(filter ? resolveFilter(filter) : [])
                ]
            }
        },
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

    const data = page.map(bucket => {

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

        const tag = Object.fromEntries(Object.entries({namespace, key, value}).map((keyValue) => {
            const key = keyValue[0]
            const value = keyValue[1]
            if (value === '' && key !== 'key') {
                return [key,null];
            }

            return [key,value];
        }))

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
}

const TAG_ORDER_BY_MAPPING: { [key: string]: string } = {
    count: '_count',
    tag: '_key'
};

export async function runQuery(query: any, id: string): Promise<any> {
    Logger.debug('executing query', ['query', query]);

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
        Logger.error(err);

        const reason = err.meta.body.error.root_cause[0].reason || ''
        if (reason.startsWith('Result window is too large')) {
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

            // return an empty response (same behavior as when there is no host
            // at the specified offset within result window)
            return countQueryRes;
        }

        throw new ElasticSearchError(err);
    }
}

start();
