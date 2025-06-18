# Use the latest LTS Node.js with security updates
FROM node:20-alpine3.19

# Install security updates and dumb-init for proper signal handling
RUN apk update && apk upgrade && \
    apk add --no-cache dumb-init && \
    rm -rf /var/cache/apk/*

# Install Python3, pip, and build tools
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-virtualenv \
    build-base \
    python3-dev

# Create Python virtual environment and install packages
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python packages in virtual environment
RUN pip install --no-cache-dir nltk spacy && \
    python3 -c "import nltk; nltk.download('punkt', download_dir='/opt/venv/share/nltk_data')" && \
    python3 -m spacy download en_core_web_sm

# Set NLTK data path
ENV NLTK_DATA="/opt/venv/share/nltk_data"

# Set the working directory in the container
WORKDIR /app

# Create a non-root user first
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodeuser -u 1001 -G nodejs

# Change ownership of the app directory to the nodejs user
RUN chown -R nodeuser:nodejs /app

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