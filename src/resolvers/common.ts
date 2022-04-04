import {
    checkLimit,
    checkOffset,
    defaultValue,
    ElasticSearchClient, extractPage,
    graphqlFiltersToESFilters
} from "xjoin-subgraph-utils";
import config from "config";

export type enumerationResolverArgs = {
    body: Record<string, any>,
    limit: number,
    offset: number,
    coreGraphqlSchema: any,
    hostFilter: any,
    orderByMapping: any,
    orderBy: string,
    orderHow: string
}

export type enumerationResolverResponse = {
    page: any,
    meta: {
        count: number,
        total: number
    }
}

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
                    "field": "host.tags_search",
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

export async function resolveEnumeration(args: enumerationResolverArgs): Promise<enumerationResolverResponse> {
    checkLimit(args.limit);
    checkOffset(args.offset);

    if (args.orderBy && !args.orderByMapping.hasOwnProperty(args.orderBy)) {
        throw new Error(`invalid order_by parameter: ${args.orderBy}`);
    }

    const validOrderHow = ['asc', 'desc'];
    if (args.orderHow && !(validOrderHow.includes(args.orderHow.toLowerCase()))) {
        throw new Error(`invalid order_how parameter: ${args.orderHow}`);
    }

    const order: Record<string, string>[] = []

    if (args.orderBy && args.orderHow) {
        order.push({[args.orderByMapping[String(args.orderBy)]]: String(args.orderHow)})
    } else if (args.orderBy && !args.orderHow) {
        order.push({[args.orderByMapping[String(args.orderBy)]]: 'ASC'})
    } else if (!args.orderBy && args.orderHow) {
        order.push({_count: String(args.orderHow)})
    }

    order.push({_key: 'ASC'}); // for deterministic sort order
    args.body.aggs.terms.terms.order = order;

    const limit = defaultValue(args.limit, 10);
    const offset = defaultValue(args.offset, 0);

    args.body.query = {
        bool: {
            filter: graphqlFiltersToESFilters(['host'], args.hostFilter, args.coreGraphqlSchema)
        }
    };
    args.body['_source'] = [];
    args.body.size = 0;

    const username: string = config.get('ElasticSearch.Username');
    const password: any = config.get('ElasticSearch.Password');

    const esClient = new ElasticSearchClient({
        node: `${config.get('ElasticSearch.URL')}`,
        auth: {
            username: username,
            password: password
        }
    }, config.get('ElasticSearch.Index'));

    const result = await esClient.runQuery({
        index: config.get('ElasticSearch.Index'),
        body: args.body
    });

    const page = extractPage(
        result?.body?.aggregations?.terms?.buckets || [],
        limit,
        offset
    );

    return {
        page: page,
        meta: {
            count: page.length,
            total: result?.body?.aggregations?.terms?.buckets?.length || 0
        }
    }
}