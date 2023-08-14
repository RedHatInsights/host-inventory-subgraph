import {
    checkLimit,
    checkOffset,
    defaultValue,
    ElasticSearchClient, extractPage,
    graphqlFiltersToESFilters
} from "xjoin-subgraph-utils";
import config from "config";
import {ClientOptions} from "@elastic/elasticsearch";

export type resolveEnumerationArgs = {
    body: Record<string, any>,
    limit: number,
    offset: number,
    coreGraphqlSchema: any,
    hostFilter: any,
    orderByMapping: any,
    orderBy: string,
    orderHow: string
}

export type resolveEnumerationResponse = {
    page: any,
    meta: {
        count: number,
        total: number
    }
}

export async function resolveEnumeration(args: resolveEnumerationArgs): Promise<resolveEnumerationResponse> {
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

    const clientOptions: ClientOptions = {
        node: `${config.get('ElasticSearch.URL')}`,
    };
    if (config.get('ElasticSearch.Username') !== "" && config.get('ElasticSearch.Password') !== "") {
        clientOptions.auth = {
            username: config.get('ElasticSearch.Username'),
            password: config.get('ElasticSearch.Password')
        }
    }
    const esClient = new ElasticSearchClient(clientOptions, config.get('ElasticSearch.Index'));

    const result = await esClient.rawQuery({
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