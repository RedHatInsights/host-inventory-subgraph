FROM registry.access.redhat.com/ubi8/nodejs-16

USER 1001

ADD src/ src/
ADD package.json package.json
ADD package-lock.json package-lock.json

RUN npm ci --ignore-scripts && npm run build

EXPOSE 4000

ENV NODE_CONFIG_DIR=dist/config

CMD [ "node", "dist/index.js" ]
