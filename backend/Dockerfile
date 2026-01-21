# Use official Node.js image
FROM node:18

# Set working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install all dependencies
RUN npm install

# Copy the rest of your app code
COPY . .

# Expose the port your Express backend runs on
EXPOSE 3000

# Start your backend app
CMD ["npm", "start"]
