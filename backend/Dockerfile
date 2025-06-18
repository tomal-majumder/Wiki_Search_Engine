# Use the latest LTS Node.js with security updates
FROM node:22-alpine

# Install security updates and dumb-init for proper signal handling
RUN apk update && apk upgrade && \
    apk add --no-cache dumb-init && \
    rm -rf /var/cache/apk/*

# Set the working directory in the container
WORKDIR /app

# Create a non-root user first
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodeuser -u 1001 -G nodejs

# Copy package files with correct ownership
COPY --chown=nodeuser:nodejs package*.json ./

# Switch to non-root user for npm install
USER nodeuser

# Install dependencies
RUN npm ci --only=production && npm cache clean --force

# Copy the rest of the application code with correct ownership
COPY --chown=nodeuser:nodejs . .

# Expose the port the app runs on
EXPOSE 3001

# Use dumb-init to handle signals properly
ENTRYPOINT ["dumb-init", "--"]

# Define the command to run the application
CMD ["node", "./bin/www"]