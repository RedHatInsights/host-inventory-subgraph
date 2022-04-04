import {resolveEnumeration, enumerationResolverResponse} from "./common.js";

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

    const enumerationResponse: enumerationResolverResponse = await resolveEnumeration({
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