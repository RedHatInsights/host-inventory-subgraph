FROM registry.access.redhat.com/ubi8/nodejs-16

USER 1001

ADD . $HOME

RUN npm ci --ignore-scripts && tsc --project tsconfig.build.json

EXPOSE 4000

ENV NODE_CONFIG_DIR=dist/config

CMD [ "node", "dist/index.js" ]
