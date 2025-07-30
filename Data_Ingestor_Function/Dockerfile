# Use the official Node.js 18 image as the base
FROM node:18-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json (if exists)
# This allows Docker to cache the npm install step
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of your application's source code
COPY . .

# Expose the port that your application will listen on
# Cloud Run uses the PORT environment variable, which is usually 8080
EXPOSE 8080

# Define the command to run your application
# This will start your Node.js server
CMD [ "node", "index.js" ]