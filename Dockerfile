FROM registry.access.redhat.com/ubi8/nodejs-16

USER root

RUN npm install -g npm@8.6.0

ADD . $HOME
RUN npm ci --only=production --ignore-scripts && tsc --project tsconfig.build.json

USER 1001

EXPOSE 4000

ENV NODE_CONFIG_DIR=dist/config

CMD [ "node", "dist/index.js" ]
