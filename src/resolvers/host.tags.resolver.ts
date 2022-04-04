import {resolveEnumeration, resolveEnumerationResponse} from "./resolve.enumeration.js";

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

    const enumerationResponse: resolveEnumerationResponse = await resolveEnumeration({
        body: body,
        limit: args.limit,
        offset: args.offset,
        coreGraphqlSchema: this.coreGraphqlSchema,
        hostFilter: args.hostFilter,
        orderByMapping: TAG_ORDER_BY_MAPPING,
        orderBy: args.order_by,
        orderHow: args.order_how
    });

    //convert string tags into tag objects, e.g.
    //convert "NS1/key1=val1" to
    //{
    //  "namespace": "NS1",
    //  "key": "key1",
    //  "value": "val1"
    //}
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