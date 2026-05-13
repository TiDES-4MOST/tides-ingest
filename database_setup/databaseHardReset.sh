#!/bin/bash

# Path to the .env file relative to this script
ENV_FILE="../.env"

# Load variables from .env if it exists
if [ -f "$ENV_FILE" ]; then
    DB_HOST=$(grep "^\s*TIDES_DB_HOST=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
    DB_USER=$(grep "^\s*TIDES_DB_USER=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
    DB_NAME=$(grep "^\s*TIDES_DB_DATABASE=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
    DB_PORT=$(grep "^\s*TIDES_DB_PORT=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\"")
else
    echo -e "\033[0;31mERROR: .env file not found at $ENV_FILE. Hard reset aborted.\033[0m"
    exit 1
fi

# Ensure all variables were found - No defaults allowed for the Nuclear Option
if [[ -z "$DB_HOST" || -z "$DB_USER" || -z "$DB_NAME" || -z "$DB_PORT" ]]; then
    echo -e "\033[0;31mERROR: Missing database credentials in .env file. Hard reset aborted.\033[0m"
    echo "Required: TIDES_DB_HOST, TIDES_DB_USER, TIDES_DB_DATABASE, TIDES_DB_PORT"
    exit 1
fi

HOST=$DB_HOST
USER=$DB_USER
NAME=$DB_NAME
PORT=$DB_PORT

# ANSI color codes
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# ASCII Art Banner
echo -e "${RED}"
echo "  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "   _    _          _____  _____     _____  ______  _____ ______ _______ "
echo "  | |  | |   /\   |  __ \|  __ \   |  __ \|  ____|/ ____|  ____|__   __|"
echo "  | |__| |  /  \  | |__) | |  | |  | |__) | |__  | (___ | |__     | |   "
echo "  |  __  | / /\ \ |  _  /| |  | |  |  _  /|  __|  \___ \|  __|    | |   "
echo "  | |  | |/ ____ \| | \ \| |__| |  | | \ \| |____ ____) | |____   | |   "
echo "  |_|  |_/_/    \_\_|  \_\_____/   |_|  \_\______|_____/|______|  |_|   "
echo "  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo -e "${NC}"

echo -e "${RED}  WARNING: THIS WILL DELETE ALL DATA IN THE TIDES MASTER TABLE${NC}"
echo -e "${RED}           AND RESET ALL NAMING SEQUENCES TO ZERO.${NC}"
echo "  Targeting: $NAME on $HOST:$PORT"
echo ""

# CHECK 1
read -p "  [CHECK 1/3] Are you absolutely sure you want to WIPE '$NAME' on $HOST:$PORT? (y/n): " confirm1
if [[ $confirm1 != [yY] ]]; then
    echo -e "\n  ${GREEN}Wise choice. Aborting.${NC}\n"
    exit 1
fi

# CHECK 2
echo -e "\n  ${YELLOW}  Wait! This is your second warning.${NC}"
read -p "  [CHECK 2/3] All survey data on $HOST:$PORT ($NAME) will be lost. Proceed? (y/n): " confirm2
if [[ $confirm2 != [yY] ]]; then
    echo -e "\n  ${GREEN}Aborting reset.${NC}\n"
    exit 1
fi

# CHECK 3
echo -e "\n  ${RED}  CHRIS FROHMAIER IS GIVING YOU A FINAL WARNING.${NC}"
echo -e "${RED}  ARE YOU ABSOLUTELY SURE YOU WANT TO DESTROY '$NAME' ON $HOST:$PORT?${NC}"
read -p "  [CHECK 3/3] Type 'RESET' to confirm nuclear wipe: " confirm3
if [[ $confirm3 != "RESET" ]]; then
    echo -e "\n  ${GREEN}Incorrect phrase. Reset cancelled.${NC}\n"
    exit 1
fi

echo -e "\n  ${RED}Executing Nuclear Wipe on $NAME ($HOST:$PORT)...${NC}\n"

# SQL Commands
SQL_COMMANDS="
TRUNCATE tides_master RESTART IDENTITY CASCADE;
ALTER SEQUENCE tides_seq RESTART WITH 0;
"

psql -h "$HOST" -p "$PORT" -U "$USER" -d "$NAME" -c "$SQL_COMMANDS"

echo -e "\n  ${GREEN}Hard reset complete. Naming sequence for '$NAME' is back at 'aaa'.${NC}\n"
