import morgan, { StreamOptions } from "morgan";

import { IncomingMessage } from "http";

import Logger from "../logging/logger";
import * as http from "http";

interface Request extends IncomingMessage {
    body: {
        query: String;
    };
}

const stream: StreamOptions = {
    write: (message) =>
        Logger.http(message.substring(0, message.lastIndexOf("\n"))),
};

const skip = (req: Request, res: http.ServerResponse): boolean => {
    //IntrospectionQueries are noisy, skip logging them
    if (req.method === 'OPTIONS') {
        return true;
    } else {
        return req.body !== undefined && req.body.query !== undefined && req.body.query.indexOf('IntrospectionQuery') !== -1;
    }
};

const registerGraphQLToken = () => {
    morgan.token("graphql-query", (req: Request) => {
        if (req !== undefined && req.body !== undefined) {
            return `GraphQL ${req.body.query}`
        }
    });
};

registerGraphQLToken();

const morganMiddleware = morgan(
    ":method :url :status :res[content-length] - :response-time ms\n:graphql-query",
    { stream, skip }
);

export default morganMiddleware;
