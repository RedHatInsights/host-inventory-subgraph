import {
    checkLimit,
    checkOffset,
    defaultValue, ElasticSearchClient,
    extractPage,
    graphqlFiltersToESFilters
} from "xjoin-subgraph-utils";
import config from "config";

export async function hostTagsResolver(this: any, parent: any, args: any, context: any): Promise<Record<string, unknown>> {
    const TAG_ORDER_BY_MAPPING: { [key: string]: string } = {
        count: '_count',
        tag: '_key'
    };

    checkLimit(args.limit);
    checkOffset(args.offset);

    const limit = defaultValue(args.limit, 10);
    const offset = defaultValue(args.offset, 0);

    const esFilters = []
    const filter = graphqlFiltersToESFilters(['host'], args.hostFilter, esFilters, this.coreGraphqlSchema);

    const body: any = {
        _source: [],
        query: {
            bool: {
                filter
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

    const esClient = new ElasticSearchClient({
        node: `${config.get('ElasticSearch.URL')}`,
        auth: {
            username: config.get('ElasticSearch.Username'),
            password: config.get('ElasticSearch.Password')
        }
    }, config.get('ElasticSearch.Index'));

    const result = await esClient.runQuery({
        index: config.get('ElasticSearch.Index'),
        body
    });

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

export async function hostOperatingSystemsResolver(this: any, parent: any, args: any, context: any): Promise<Record<string, unknown>> {
    const ORDER_BY_MAPPING: { [key: string]: string } = {
        count: '_count',
        operating_system: '_key'
    };

    checkLimit(args.limit);
    checkOffset(args.offset);

    const limit = defaultValue(args.limit, 10);
    const offset = defaultValue(args.offset, 0);

    const esFilters = []
    const filter = graphqlFiltersToESFilters(['host'], args.hostFilter, esFilters, this.coreGraphqlSchema);

    const delimiter = '||||';

    const body: any = {
        _source: [],
        query: {
            bool: {
                filter
            }
        },
        size: 0,
        aggs: {
            os: {
                terms: {
                    script: `if(doc['host.system_profile_facts.operating_system.name'].size()!=0 && doc['host.system_profile_facts.operating_system.major'].size()!=0 && doc['host.system_profile_facts.operating_system.minor'].size()!=0){return doc['host.system_profile_facts.operating_system.name'].value + '${delimiter}' + doc['host.system_profile_facts.operating_system.major'].value + '${delimiter}' + doc['host.system_profile_facts.operating_system.minor'].value;}`,
                    size: 10000, //TODO
                    order: [{
                        [ORDER_BY_MAPPING[String(args.order_by)]]: String(args.order_how)
                    }, {
                        _key: 'ASC' // for deterministic sort order
                    }],
                    show_term_doc_count_error: true
                }
            }
        }
    };

    const esClient = new ElasticSearchClient({
        node: `${config.get('ElasticSearch.URL')}`,
        auth: {
            username: config.get('ElasticSearch.Username'),
            password: config.get('ElasticSearch.Password')
        }
    }, config.get('ElasticSearch.Index'));

    const result = await esClient.runQuery({
        index: config.get('ElasticSearch.Index'),
        body
    });

    const page = extractPage(
        result.body.aggregations.os.buckets,
        limit,
        offset
    );

    const data = page.map(bucket => {
        // Toss unknown and incomplete OS versions
        if (bucket.key === '') {
            return;
        }

        const versionSplit = bucket.key.split('||||');
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
        meta: {
            count: data.length,
            total: result.body.aggregations.os.buckets.length
        }
    };
}

