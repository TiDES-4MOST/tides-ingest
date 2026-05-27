#!/bin/bash

# Path to the .env file relative to this script
ENV_FILE="../.env"

# Default values
DEFAULT_HOST="192.168.10.48"
DEFAULT_USER="tidesadmin"
DEFAULT_NAME="tides"

# Load variables from .env if it exists
if [ -f "$ENV_FILE" ]; then
    DB_HOST=$(grep "^\s*TIDES_DB_HOST=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
    DB_USER=$(grep "^\s*TIDES_DB_USER=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
    DB_NAME=$(grep "^\s*TIDES_DB_DATABASE=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
fi

# Determine final values and track if defaults are used
HOST=${DB_HOST:-$DEFAULT_HOST}
USER=${DB_USER:-$DEFAULT_USER}
NAME=${DB_NAME:-$DEFAULT_NAME}

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# ASCII Art Banner
echo -e "${GREEN}"
echo "  ================================================================"
echo "    _____  ____     _____ ______ _______ _    _ _____  "
echo "   |  __ \|  _ \   / ____|  ____|__   __| |  | |  __ \ "
echo "   | |  | | |_) | | (___ | |__     | |  | |  | | |__) |"
echo "   | |  | |  _ <   \___ \|  __|    | |  | |  | |  ___/ "
echo "   | |__| | |_) |  ____) | |____   | |  | |__| | |     "
echo "   |_____/|____/  |_____/|______|  |_|   \____/|_|     "
echo "  ================================================================"
echo -e "${NC}"

echo "  Target Database Configuration:"
echo "  ------------------------------"
printf "  %-12s %s" "HOST:" "$HOST"
if [ "$HOST" == "$DEFAULT_HOST" ]; then echo -e " ${RED}(DEFAULT)${NC}"; else echo ""; fi

printf "  %-12s %s" "USER:" "$USER"
if [ "$USER" == "$DEFAULT_USER" ]; then echo -e " ${RED}(DEFAULT)${NC}"; else echo ""; fi

printf "  %-12s %s" "DATABASE:" "$NAME"
if [ "$NAME" == "$DEFAULT_NAME" ]; then echo -e " ${RED}(DEFAULT)${NC}"; else echo ""; fi

echo ""
if [[ "$HOST" == "$DEFAULT_HOST" || "$USER" == "$DEFAULT_USER" || "$NAME" == "$DEFAULT_NAME" ]]; then
    echo -e "${RED}  WARNING: One or more values are using hardcoded defaults!${NC}"
fi

echo ""
read -p "  Are you happy to proceed with these settings? (y/n): " confirm

if [[ $confirm != [yY] ]]; then
    echo -e "\n  ${RED}Aborting database setup.${NC}\n"
    exit 1
fi

echo -e "\n  ${GREEN}Proceeding with database setup...${NC}\n"

psql -h "$HOST" -U "$USER" -d "$NAME" -f createMasterTable.sql
psql -h "$HOST" -U "$USER" -d "$NAME" -f surveyIDs.sql
psql -h "$HOST" -U "$USER" -d "$NAME" -f surveyConnector.sql
psql -h "$HOST" -U "$USER" -d "$NAME" -f pipelines.sql
psql -h "$HOST" -U "$USER" -d "$NAME" -f pipelineSelections.sql
psql -h "$HOST" -U "$USER" -d "$NAME" -f createHostTable.sql
