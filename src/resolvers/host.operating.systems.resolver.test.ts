import 'reflect-metadata';
import {
    argumentTest, ElasticsearchAggregation, ElasticsearchBucket,
    elasticsearchRequestTemplate,
    elasticsearchResponseTemplate,
    invalidArgumentTest
} from "./common.test.js";
import {hostOperatingSystemsResolver} from "./host.operating.systems.resolver.js";

const delimiter = '||||';

async function operatingSystemsArgumentTest(
    gqlArguments: Record<any, any>,
    elasticsearchRequestBody: Record<any, any>,
    elasticsearchResponseBody?: Record<any, any>,
    gqlResponse?: Record<any, any>) {

    await argumentTest(hostOperatingSystemsResolver, gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, gqlResponse);
}

async function operatingSystemsInvalidArgumentTest(
    gqlArguments: Record<any, any>,
    exceptionMessage: string) {

    await invalidArgumentTest(hostOperatingSystemsResolver, gqlArguments, exceptionMessage);
}

function operatingSystemElasticsearchRequest(): Record<any, any> {
    const template = elasticsearchRequestTemplate();
    template.aggs.terms.terms.script = `if(doc['host.system_profile_facts.operating_system.name'].size()!=0 && doc['host.system_profile_facts.operating_system.major'].size()!=0 && doc['host.system_profile_facts.operating_system.minor'].size()!=0){return doc['host.system_profile_facts.operating_system.name'].value + '${delimiter}' + doc['host.system_profile_facts.operating_system.major'].value + '${delimiter}' + doc['host.system_profile_facts.operating_system.minor'].value;}`;
    return template;
}

interface OperatingSystemGqlResponseData {
    operating_system: {
        name: string,
        major: string,
        minor: string
    },
    count: number
}

interface OperatingSystemsGqlResponse {
    data: OperatingSystemGqlResponseData[],
    meta: {
        count: number,
        total: number
    }
};

interface GeneratedOperatingSystems {
    elasticsearchAggregation: ElasticsearchAggregation,
    gqlResponse: OperatingSystemsGqlResponse
}

function generateOperatingSystems(total: number, limit: number, offset: number): GeneratedOperatingSystems {
    const buckets: ElasticsearchBucket[] = [];
    const gqlResponseData: OperatingSystemGqlResponseData[] = [];
    for (let i = 0; i < total; i++) {
        buckets.push({
            key: `RHEL${delimiter}${i}${delimiter}${i}`,
            doc_count: i,
            doc_count_error_upper_bound: 0
        })

        gqlResponseData.push({
            operating_system: {
                name: `RHEL`,
                major: `${i}`,
                minor: `${i}`
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


describe('hostOperatingSystemResolver', () => {
    describe('order', () => {
        test('returns no results when Elasticsearch index is empty', async () => {
            await operatingSystemsArgumentTest({}, operatingSystemElasticsearchRequest());
        });

        test('transforms order_by: count argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_by': 'count'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'ASC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_by: operating_system argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_by': 'operating_system'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': 'ASC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: DESC argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'DESC'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'DESC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: ASC argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'ASC'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'ASC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: ASC, order_by: operating_system argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'ASC',
                'order_by': 'operating_system'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': 'ASC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: DESC, order_by: operating_system argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'DESC',
                'order_by': 'operating_system'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': 'DESC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: ASC, order_by: count argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'ASC',
                'order_by': 'count'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'ASC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms order_how: DESC, order_by: count argument into elasticsearch query', async () => {
            const gqlArguments = {
                'order_how': 'DESC',
                'order_by': 'count'
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_count': 'DESC'});
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('rejects invalid order_by argument', async () => {
            const gqlArguments = {
                order_by: 'invalid'
            }
            await operatingSystemsInvalidArgumentTest(gqlArguments, 'invalid order_by parameter: invalid');
        });

        test('rejects invalid order_how argument', async () => {
            const gqlArguments = {
                order_how: 'invalid'
            }
            await operatingSystemsInvalidArgumentTest(gqlArguments, 'invalid order_how parameter: invalid');
        });
    });

    describe('pagination', () => {
        test('rejects invalid limit argument', async () => {
            const gqlArguments = {
                limit: 101
            }
            await operatingSystemsInvalidArgumentTest(gqlArguments, 'value must be 100 or less (was 101)');
        });

        test('rejects invalid offset argument', async () => {
            const gqlArguments = {
                offset: -1
            }
            await operatingSystemsInvalidArgumentTest(gqlArguments, 'value must be 0 or greater (was -1)');
        });

        test('correctly applies limit argument', async () => {
            const limit = 1;
            const offset = 0;
            const total = 3;

            const gqlArguments = {
                limit: limit
            }
            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });

        test('correctly applies offset argument', async () => {
            const offset = 1;
            const limit = 10;
            const total = 3;

            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const gqlArguments = {
                offset: offset
            }
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;

            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });

        test('correctly applies limit and offset argument', async () => {
            const limit = 1;
            const offset = 1;
            const total = 3;

            const gqlArguments = {
                offset: offset,
                limit: limit
            }
            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;

            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });

        test('correctly applies limit and offset with order_by argument', async () => {
            const offset = 2;
            const limit = 1;
            const total = 3;
            const order_by = 'operating_system';
            const order_how = 'ASC';

            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const gqlArguments = {
                offset: offset,
                limit: limit,
                order_by: order_by
            }
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;

            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });

        test('correctly applies limit and offset with order_by and order_how arguments', async () => {
            const limit = 1;
            const offset = 2;
            const total = 3;
            const order_by = 'operating_system';
            const order_how = 'DESC';

            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const gqlArguments = {
                offset: offset,
                limit: limit,
                order_by: order_by,
                order_how: order_how
            }
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;

            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });

        test('sets the default limit to 10 when no limit argument is present', async () => {
            const offset = 1;
            const limit = 10;
            const total = 11;
            const order_by = 'operating_system';
            const order_how = 'DESC'

            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const gqlArguments = {
                offset: offset,
                order_by: order_by,
                order_how: order_how
            }
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;

            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });

        test('sets the default offset to 0 when no offset argument is present', async () => {
            const offset = 0;
            const limit = 5;
            const total = 11;
            const order_by = 'operating_system';
            const order_how = 'DESC';

            const generatedOperatingSystems = generateOperatingSystems(total, limit, offset);
            const gqlArguments = {
                limit: limit,
                order_by: order_by,
                order_how: order_how
            }
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.aggs.terms.terms.order.unshift({'_key': order_how});
            const elasticsearchResponseBody = elasticsearchResponseTemplate();
            elasticsearchResponseBody.aggregations = generatedOperatingSystems.elasticsearchAggregation;

            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody, elasticsearchResponseBody, generatedOperatingSystems.gqlResponse);
        });
    });

    describe('host filter', () => {
        test('transforms hostFilter argument into elasticsearch query', async () => {
            const hostId = '1234';
            const gqlArguments = {
                hostFilter: {
                    id: {
                        eq: hostId
                    }
                },
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.query = {
                bool: {
                    filter: [{
                        term: {
                            'host.id': hostId
                        }
                    }]
                }
            }
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

        test('transforms nested hostFilter argument into elasticsearch query', async () => {
            const osName = 'RHEL';
            const gqlArguments = {
                hostFilter: {
                    system_profile_facts: {
                        operating_system: {
                            name: {
                                eq: osName
                            }
                        }
                    }
                }
            };
            const elasticsearchRequestBody = operatingSystemElasticsearchRequest();
            elasticsearchRequestBody.query = {
                bool: {
                    filter: [{
                        term: {
                            'host.system_profile_facts.operating_system.name': osName
                        }
                    }]
                }
            }
            await operatingSystemsArgumentTest(gqlArguments, elasticsearchRequestBody);
        });

    });
});
