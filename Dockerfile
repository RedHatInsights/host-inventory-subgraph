FROM registry.redhat.io/ubi8/nodejs-16

USER root

ADD . $HOME
RUN npm ci --only=production --ignore-scripts && tsc

USER 1001

EXPOSE 4000

ENV NODE_CONFIG_DIR=dist/config

CMD [ "node", "dist/index.js" ]
