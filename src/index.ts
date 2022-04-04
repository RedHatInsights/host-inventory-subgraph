import 'reflect-metadata';
import {buildFederatedSchema} from '@apollo/federation';
import {ApolloServer, gql} from 'apollo-server-express';
import express from 'express';
import config from 'config';
import { readFileSync } from 'fs';
import {
    SchemaRegistry,
    morganMiddleware,
    Logger,
    GraphqlSchema,
    AvroSchemaParser
} from "xjoin-subgraph-utils";
import {hostOperatingSystemsResolver} from "./resolvers/host.operating.systems.resolver.js";
import {hostTagsResolver} from "./resolvers/host.tags.resolver.js";

async function start() {
    const app = express();

    //load the core graphql schema (the graphql schema that is generated from an avro schema)
    const avroSchemaParser = new AvroSchemaParser(config.get("AvroSchema"));
    const coreGraphqlSchema: GraphqlSchema = avroSchemaParser.convertToGraphQL();

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
            HostTags: hostTagsResolver.bind({coreGraphqlSchema}),
            HostOperatingSystems: hostOperatingSystemsResolver.bind({coreGraphqlSchema})
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

start();
