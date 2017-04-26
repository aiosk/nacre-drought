#!/bin/sh
docker exec -it $(docker ps | grep nacredrought_scraper | awk '{print $1}') bash
