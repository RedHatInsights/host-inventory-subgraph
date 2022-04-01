import {
    checkLimit,
    checkOffset,
    defaultValue,
    ElasticSearchClient,
    extractPage,
    graphqlFiltersToESFilters
} from "xjoin-subgraph-utils";
import config from "config";

export async function hostTagsResolver(this: any, parent: any, args: any): Promise<Record<string, unknown>> {
    const TAG_ORDER_BY_MAPPING: { [key: string]: string } = {
        count: '_count',
        tag: '_key'
    };

    const body: any = {
        aggs: {
            terms: {
                terms: {
                    field: 'host.tags_search',
                    size: 10000, //TODO
                    show_term_doc_count_error: true
                }
            }
        }
    };

    if (args.filter && args.filter.search) {
        const search = args.filter.search;
        if (search.eq) {
            body.aggs.terms.terms.include = [search.eq];
        } else if (search.regex) {
            body.aggs.terms.terms.include = search.regex;
        }
    }

    const enumerationResponse: enumerationResolverResponse = await enumerationResolver({
        body: body,
        limit: args.limit,
        offset: args.offset,
        coreGraphqlSchema: this.coreGraphqlSchema,
        hostFilter: args.hostFilter,
        orderByMapping: TAG_ORDER_BY_MAPPING,
        orderBy: args.order_by,
        orderHow: args.order_how
    });

    const data = enumerationResponse.page.map(bucket => {
        function split(value: string, delimiter: string) {
            const index = value.indexOf(delimiter);

            if (index === -1) {
                throw new Error(`cannot split ${value} using ${delimiter}`);
            }

            return [value.substring(0, index), value.substring(index + 1)];
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
        meta: enumerationResponse.meta
    };
}

export async function hostOperatingSystemsResolver(this: any, parent: any, args: any): Promise<Record<string, unknown>> {
    const ORDER_BY_MAPPING: { [key: string]: string } = {
        count: '_count',
        operating_system: '_key'
    };

    const delimiter = '||||';

    const body: any = {
        aggs: {
            terms: {
                terms: {
                    script: `if(doc['host.system_profile_facts.operating_system.name'].size()!=0 && doc['host.system_profile_facts.operating_system.major'].size()!=0 && doc['host.system_profile_facts.operating_system.minor'].size()!=0){return doc['host.system_profile_facts.operating_system.name'].value + '${delimiter}' + doc['host.system_profile_facts.operating_system.major'].value + '${delimiter}' + doc['host.system_profile_facts.operating_system.minor'].value;}`,
                    size: 10000, //TODO
                    show_term_doc_count_error: true
                }
            }
        }
    };

    const enumerationResponse: enumerationResolverResponse = await enumerationResolver({
        body: body,
        limit: args.limit,
        offset: args.offset,
        coreGraphqlSchema: this.coreGraphqlSchema,
        hostFilter: args.hostFilter,
        orderByMapping: ORDER_BY_MAPPING,
        orderBy: args.order_by,
        orderHow: args.order_how
    });

    const data = enumerationResponse.page.map(bucket => {
        // Toss unknown and incomplete OS versions
        if (bucket.key === '') {
            return;
        }

        const versionSplit = bucket.key.split(delimiter);
        return {
            operating_system: {
                name: versionSplit[0],
                major: versionSplit[1],
                minor: versionSplit[2]
            },
            count: bucket.doc_count
        };
    });

    return {
        data,
        meta: enumerationResponse.meta
    };
}

type enumerationResolverArgs = {
    body: Record<string, any>,
    limit: number,
    offset: number,
    coreGraphqlSchema: any,
    hostFilter: any,
    orderByMapping: any,
    orderBy: string,
    orderHow: string
}

type enumerationResolverResponse = {
    page: any,
    meta: {
        count: number,
        total: number
    }
}

export async function enumerationResolver(args: enumerationResolverArgs): Promise<enumerationResolverResponse> {
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
