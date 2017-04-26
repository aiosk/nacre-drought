# nacre-drought

### Run container
	$ ./docker-compose-build.sh
	$ ./docker-compose-up.sh

### Run url generator (only once)
	$ ./docker-compose-login.sh
	$ python url.py
	$ exit

### Run scraper
	$ ./docker-compose-login.sh
	$ python main.py

you can stop scrapper process by pressing `CTRL+C`. you can continue scrapper using `$ python main.py`

Get your data on `./scraper/data.json`
