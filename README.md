## Web Scraper with Rotating Proxy and SQLite Database
This Python script is a web scraper designed to extract information about real estate offers from the Otodom website in Wroclaw, Poland. It utilizes rotating proxies for web scraping to avoid IP bans and a SQLite database for managing proxy lists. Due to frequent changes in CSS tags, the code may need to be modified before use.

## Features
Rotating proxies to prevent IP blocking during web scraping.
SQLite database integration for storing and managing proxies.
User-agent rotation to mimic different web browsers.
Randomized headers, including referer, to simulate more realistic web browsing behavior.
Logging of key events for monitoring and debugging purposes.
Retry mechanism for fetching web pages in case of failures.

## Usage
Set up the SQLite database:
Download a list of proxies (ex. https://free-proxy-list.net/):
Create a text file containing a list of proxies, one per line (e.g., proxy_list.txt).
Run the db_module.py script to create the necessary tables for storing proxies.
Run the web_scraper.py 
The script will fetch real estate offers from Otodom, extract relevant information, and store it in a CSV file named oto_dom_wroclaw_dd_mm_yyyy.

# Disclaimer!
This script is intended for educational and personal use only. Be respectful of the website's terms of service, and ensure compliance with legal and ethical standards when web scraping. The rotating proxy feature is included to minimize the risk of IP blocking, but usage should be within acceptable limits to avoid causing disruptions to the target website. Use at your own discretion.

## License
This project is licensed under the MIT License
