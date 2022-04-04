import 'reflect-metadata';
import {hostTagsResolver} from "./host.tags.resolver.js";
import {AvroSchemaParser, GraphqlSchema} from "xjoin-subgraph-utils";
import config from "config";
import nock from "nock";
import {elasticsearchRequestTemplate, elasticsearchResponseTemplate, emptyGraphQLResponse} from "./common.js";

const ES_URL: string = config.get('ElasticSearch.URL');
const ES_USERNAME: string = config.get('ElasticSearch.Username');
const ES_PASSWORD: string = config.get('ElasticSearch.Password');
const ES_INDEX: string = config.get('ElasticSearch.Index');

async function argumentTest(
    gqlArguments: Record<any, any>,
    elasticsearchRequestBody: Record<any, any>,
    elasticsearchResponseBody?: Record<any, any>,
    gqlResponse?: Record<any, any>) {

    if (!elasticsearchResponseBody) {
        elasticsearchResponseBody = elasticsearchResponseTemplate();
    }

    if (!gqlResponse) {
        gqlResponse = emptyGraphQLResponse;
    }

    const scope = nock(`${ES_URL}`)
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

    const avroSchemaParser = new AvroSchemaParser(config.get("AvroSchema"));
    const coreGraphqlSchema: GraphqlSchema = avroSchemaParser.convertToGraphQL();
    const resolver = hostTagsResolver.bind({coreGraphqlSchema: coreGraphqlSchema})
    const response = await resolver({}, gqlArguments);

    expect(response).toStrictEqual(gqlResponse);
    scope.done()
}

async function invalidArgumentTest(gqlArguments: Record<any, any>, exceptionMessage: string) {
    const avroSchemaParser = new AvroSchemaParser(config.get("AvroSchema"));
    const coreGraphqlSchema: GraphqlSchema = avroSchemaParser.convertToGraphQL();
    const resolver = hostTagsResolver.bind({coreGraphqlSchema: coreGraphqlSchema})
    await expect(resolver({}, gqlArguments)).rejects.toThrow(exceptionMessage)
}

interface TagBucket {
    key: string,
    doc_count: number,
    doc_count_error_upper_bound: number
}

interface TagElasticsearchAggregation {
    terms: {
        doc_count_error_upper_bound: number,
        sum_other_doc_count: number,
        buckets: TagBucket[]
    }
}

interface TagGqlResponseData {
    tag: {
        namespace: string,
        key: string,
        value: string
    },
    count: number
}

interface TagGqlResponse {
    data: TagGqlResponseData[],
    meta: {
        count: number,
        total: number
    }
};

interface GeneratedTags {
    elasticsearchAggregation: TagElasticsearchAggregation,
    gqlResponse: TagGqlResponse
}

function generateTags(total: number, limit: number, offset: number): GeneratedTags {
    const buckets: TagBucket[] = [];
    const gqlResponseData: TagGqlResponseData[] = [];
    for (let i = 0; i < total; i++) {
        buckets.push({
            key: `NS${i}/key${i}=val${i}`,
            doc_count: i,
            doc_count_error_upper_bound: 0
        })

        gqlResponseData.push({
            tag: {
                namespace: `NS${i}`,
                key: `key${i}`,
                value: `val${i}`
            },
            count: i
        })
    }

    const gqlResponseDataSlice = gqlResponseData.slice(offset, offset+limit);
    return {
        elasticsearchAggregation: {
            terms: {
                doc_count_error_upper_bound: 0,
                sum_other_doc_count: 0,
                buckets: buckets
            }
        },
        gqlResponse: {
            data: gqlResponseDataSlice,
            meta: {
                count: gqlResponseDataSlice.length,
                total: total
            }
        }
    }
}

describe('hostTagsResolver', () => {
    describe('order', () => {
        test('returns no results when Elasticsearch index is empty', async () => {
            await argumentTest({}, elasticsearchRequestTemplate);
        });

        test('transforms order_by: count argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_by': 'count'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'ASC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_by: tag argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_by': 'tag'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': 'ASC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: DESC argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'DESC'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'DESC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: ASC argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'ASC'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'ASC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: ASC, order_by: tag argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'ASC',
                'order_by': 'tag'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': 'ASC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: DESC, order_by: tag argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'DESC',
                'order_by': 'tag'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': 'DESC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: ASC, order_by: count argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'ASC',
                'order_by': 'count'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'ASC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: DESC, order_by: count argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'DESC',
                'order_by': 'count'
            };
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'DESC'});
            await argumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('rejects invalid order_by argument', async () => {
            const gqlArguments = {
                order_by: 'invalid'
            }
            await invalidArgumentTest(gqlArguments, 'invalid order_by parameter: invalid');
        });

        test('rejects invalid order_how argument', async () => {
            const gqlArguments = {
                order_how: 'invalid'
            }
            await invalidArgumentTest(gqlArguments, 'invalid order_how parameter: invalid');
        });
    });

    describe('pagination', () => {
        test('rejects invalid limit argument', async () => {
            const gqlArguments = {
                limit: 101
            }
            await invalidArgumentTest(gqlArguments, 'value must be 100 or less (was 101)');
        });

        test('rejects invalid offset argument', async () => {
            const gqlArguments = {
                offset: -1
            }
            await invalidArgumentTest(gqlArguments, 'value must be 0 or greater (was -1)');
        });

        test('correctly applies limit argument', async () => {
            const limit = 1;
            const offset = 0;
            const total = 3;

            const gqlArguments = {
                limit: limit
            }
            const generatedTags = generateTags(total, limit, offset);
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;
            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });

        test('correctly applies offset argument', async () => {
            const offset = 1;
            const limit = 10;
            const total = 3;

            const generatedTags = generateTags(total, limit, offset);
            const gqlArguments = {
                offset: offset
            }
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;

            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });

        test('correctly applies limit and offset argument', async () => {
            const limit = 1;
            const offset = 1;
            const total = 3;

            const gqlArguments = {
                offset: offset,
                limit: limit
            }
            const generatedTags = generateTags(total, limit, offset);
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;

            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });

        test('correctly applies limit and offset with order_by argument', async () => {
            const offset = 2;
            const limit = 1;
            const total = 3;
            const order_by = 'tag';
            const order_how = 'ASC';

            const generatedTags = generateTags(total, limit, offset);
            const gqlArguments = {
                offset: offset,
                limit: limit,
                order_by: order_by
            }
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;

            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });

        test('correctly applies limit and offset with order_by and order_how arguments', async () => {
            const limit = 1;
            const offset = 2;
            const total = 3;
            const order_by = 'tag';
            const order_how = 'DESC';

            const generatedTags = generateTags(total, limit, offset);
            const gqlArguments = {
                offset: offset,
                limit: limit,
                order_by: order_by,
                order_how: order_how
            }
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;

            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });

        test('sets the default limit to 10 when no limit argument is present', async () => {
            const offset = 1;
            const limit = 10;
            const total = 11;
            const order_by = 'tag';
            const order_how = 'DESC'

            const generatedTags = generateTags(total, limit, offset);
            const gqlArguments = {
                offset: offset,
                order_by: order_by,
                order_how: order_how
            }
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;

            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });

        test('sets the default offset to 0 when no offset argument is present', async () => {
            const offset = 0;
            const limit = 5;
            const total = 11;
            const order_by = 'tag';
            const order_how = 'DESC';

            const generatedTags = generateTags(total, limit, offset);
            const gqlArguments = {
                limit: limit,
                order_by: order_by,
                order_how: order_how
            }
            const elasticsearchRequestBody = elasticsearchRequestTemplate();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedTags.elasticsearchAggregation;

            await argumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedTags.gqlResponse);
        });
    });

    describe('host filter', () => {
        test('transforms hostfilter argument into elasticsearch query', async () => {

        });

        test('transforms nested hostfilter argument into elasticsearch query', async () => {

        });

    });

    describe('tag filter', () => {
        test('transforms filter.search.eq argument into elasticsearch query', async () => {

        });

        test('transforms filter.search.regex argument into elasticsearch query', async () => {

        });

    });
});
