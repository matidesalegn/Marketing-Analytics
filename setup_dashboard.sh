# !/bin/bash
set -e

# Function to setup Redash & Running Database Migrations
setup_redash() {
    echo "Setting up Redash..."
    docker-compose -f docker-compose.redash.yml up -d
}

# Function to setup Metabase
setup_metabase() {
    echo "Setting up Metabase..."
    docker-compose -f docker-compose.metabase.yml up -d
}

# Function to setup Superset
setup_superset() {
    echo "Setting up Superset..."
    docker-compose -f docker-compose.superset.yml up -d
}

# Check if Docker and Docker Compose are installed
if ! [ -x "$(command -v docker)" ]; then
    echo 'Error: Docker is not installed.' >&2
    exit 1
fi

if ! [ -x "$(command -v docker-compose)" ]; then
    echo 'Error: Docker Compose is not installed.' >&2
    exit 1
fi

# Parse command line arguments
case "$1" in
    redash)
        setup_redash
        ;;
    metabase)
        setup_metabase
        ;;
    superset)
        setup_superset
        ;;
    *)
        echo "Usage: $0 {redash|metabase|superset}"
        exit 1
        ;;
esac

echo "Setup complete."