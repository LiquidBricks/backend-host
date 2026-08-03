FROM node:24-bookworm-slim

WORKDIR /app

# Install git so npm can fetch GitHub dependencies during install
RUN apt-get update \
  && apt-get install -y --no-install-recommends git \
  && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV NODE_ENV=production \
  PORT=4000 \
  NATS_IP_ADDRESS=nats://127.0.0.1:4222

EXPOSE 4000

CMD ["node", "index.js"]
