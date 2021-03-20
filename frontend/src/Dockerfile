FROM node

WORKDIR /app

COPY package*.json ./

RUN npm install
# If the code is built for production, run
# RUN npm install --only=production

COPY . /app

# RUN npm run build

# CMD ["npm", "start"]