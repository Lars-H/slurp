FROM node:14.15.4-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install
# If the code is built for production, run
# RUN npm install --only=production

COPY . /app

CMD [ "npm", "start" ]