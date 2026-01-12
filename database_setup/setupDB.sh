#!/bin/bash

psql -h 192.168.10.45 -U tidesadmin -d tides -f createMasterTable.sql
psql -h 192.168.10.45 -U tidesadmin -d tides -f surveyIDs.sql
psql -h 192.168.10.45 -U tidesadmin -d tides -f surveyConnector.sql

