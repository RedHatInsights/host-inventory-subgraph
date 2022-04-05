import nock, {Scope} from "nock";
import config from "config";
import {AvroSchemaParser, GraphqlSchema} from "xjoin-subgraph-utils";

const ES_URL: string = config.get('ElasticSearch.URL');
const ES_USERNAME: string = config.get('ElasticSearch.Username');
const ES_PASSWORD: string = config.get('ElasticSearch.Password');
const ES_INDEX: string = config.get('ElasticSearch.Index');

export const emptyGraphQLResponse: Record<any, any> = {
    data: [],
    meta: {
        count: 0,
        total: 0
    }
}

export function elasticsearchResponseTemplate(): Record<any, any> {
    return {
        took: 1,
        timed_out: false,
        _shards: {
            'total': 3,
            'successful': 3,
            'skipped': 0,
            'failed': 0
        },
        hits: {
            total: {
                value: 0,
                relation: 'eq'
            },
            max_score: null,
            hits: []
        }
    }
}

export function elasticsearchRequestTemplate(): Record<any, any> {
    return {
        "aggs": {
            "terms": {
                "terms": {
                    "size": 10000,
                    "order": [
                        {
                            "_key": "ASC"
                        }
                    ],
                    "show_term_doc_count_error": true
                }
            }
        },
        "query": {
            "bool": {
                "filter": []
            }
        },
        "_source": [],
        "size": 0
    };
}

export function mockElasticsearchSearchAPICall(
    elasticsearchRequestBody?: Record<any, any>,
    elasticsearchResponseBody?: Record<any, any>,
): Scope {
    if (!elasticsearchResponseBody) {
        elasticsearchResponseBody = elasticsearchResponseTemplate();
    }

    if (!elasticsearchRequestBody) {
        elasticsearchRequestBody = {};
    }

    return nock(`${ES_URL}`)
        .post(
            `/${ES_INDEX}/_search`,
            elasticsearchRequestBody,
            {
                reqheaders: {
                    'content-type': 'application/json'
                }
            })
        .basicAuth({user: ES_USERNAME, pass: ES_PASSWORD})
        .reply(200, elasticsearchResponseBody, {'Content-Type': 'application/json'})
}

export async function argumentTest(
    resolver: any,
    gqlArguments: Record<any, any>,
    elasticsearchRequestBody: Record<any, any>,
    elasticsearchResponseBody?: Record<any, any>,
    gqlResponse?: Record<any, any>) {

    const scope = mockElasticsearchSearchAPICall(elasticsearchRequestBody, elasticsearchResponseBody);

    const avroSchemaParser = new AvroSchemaParser(config.get("AvroSchema"));
    const coreGraphqlSchema: GraphqlSchema = avroSchemaParser.convertToGraphQL();
    const resolverBound = resolver.bind({coreGraphqlSchema: coreGraphqlSchema})

    if (!gqlResponse) {
        gqlResponse = emptyGraphQLResponse;
    }

    const response = await resolverBound({}, gqlArguments);

    expect(response).toStrictEqual(gqlResponse);
    scope.done();
}

export async function invalidArgumentTest(
    resolver: any,
    exceptionMessage: string,
    gqlArguments?: Record<any, any>,
    elasticsearchRequestBody?: Record<any, any>,
    elasticsearchResponseBody?: Record<any, any>) {

    let scope;
    if (elasticsearchRequestBody && elasticsearchResponseBody) {
        scope = mockElasticsearchSearchAPICall(elasticsearchRequestBody, elasticsearchResponseBody);
    }

    const avroSchemaParser = new AvroSchemaParser(config.get("AvroSchema"));
    const coreGraphqlSchema: GraphqlSchema = avroSchemaParser.convertToGraphQL();
    const resolverBound = resolver.bind({coreGraphqlSchema: coreGraphqlSchema})

    await expect(resolverBound({}, gqlArguments)).rejects.toThrow(exceptionMessage)

    if (scope) {
        scope.done();
    }
}

export interface ElasticsearchBucket {
    key: string,
    doc_count: number,
    doc_count_error_upper_bound: number
}

export interface ElasticsearchAggregation {
    terms: {
        doc_count_error_upper_bound: number,
        sum_other_doc_count: number,
        buckets: ElasticsearchBucket[]
    }
}

