# Use the official Node.js 18 image as the base
FROM node:18-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json (if exists)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of your application's source code
COPY . .

# Expose the port that your application will listen on
EXPOSE 8080

# Define the command to run your application
CMD [ "node", "index.js" ]