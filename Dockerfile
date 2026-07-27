# ---- Build stage ----
FROM node:20-slim AS build
WORKDIR /app

# Copy package files first so this layer caches unless deps change.
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

# ---- Production stage ----
FROM node:20-slim AS production
WORKDIR /app

COPY package*.json ./
# Install only runtime deps, skip TS/types/etc.
RUN npm ci --omit=dev

# Bring compiled JS from the build stage.
COPY --from=build /app/dist ./dist

# No CMD — docker-compose sets the command per container (API vs worker).