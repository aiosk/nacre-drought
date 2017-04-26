#!/bin/sh
docker exec -it $(docker ps | grep scraper | awk '{print $1}') bash
