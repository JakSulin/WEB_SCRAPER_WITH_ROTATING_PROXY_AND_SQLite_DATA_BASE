from bs4 import BeautifulSoup
import requests
from datetime import datetime
import csv
import random
import time
import logging 
import db_module

logging.basicConfig(filename='std.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG, encoding='utf-8')
logger=logging.getLogger()

#Connect to database
db = db_module.Database("web_scraper_data_base")
if db.check_table_exists("proxies_unchecked"):
    db.drop_table("proxies_unchecked")
db.create_table("proxies_unchecked", "id INTEGER PRIMARY KEY AUTOINCREMENT", "ip_address")
file_url = "proxy_list.txt"
#Append proxies to data base
db_module.download_proxies_from_file(db, file_url)


VALID_STATUSES = [200, 301, 302, 307, 404]  
user_agent_list = [ 
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36', 
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', 
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15', 
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0',
                    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188',
                    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36']

referer_list = ['https://www.polsatnews.pl/',
                'https://www.facebook.com/',
                'https://tvn24.pl/',
                'https://businessinsider.com.pl/',
                'https://www.money.pl/gospodarka/ceny-mieszkan-wskazniki-cen-lokali-mieszkalnych-w-i-kwartale-2023-r-dane-gus-6916275925240608a.html',
                'https://www.money.pl/gospodarka/ceny-mieszkan-szaleja-a-lokali-zaczyna-brakowac-co-sie-dzieje-6955196972055040a.html',
                'https://www.morizon.pl/blog/indeks-cen-mieszkan/',
                'https://www.bankier.pl/wiadomosc/Podwyzki-cen-mieszkan-nie-odpuszcza-Podaz-nie-nadaza-za-sztucznie-rozdmuchanym-popytem-8614581.html',
                'https://tvn24.pl/biznes/z-kraju/ceny-mieszkan-eksplodowaly-dane-za-wrzesien-2023-analiza-7359890',
                'https://tvn24.pl/biznes/z-kraju/ceny-mieszkan-w-polsce-pazdziernik-2023-w-tych-miastach-wzrosty-sa-najwieksze-7410380',
                'https://www.rp.pl/nieruchomosci/art39048221-rzadowy-kredyt-2-nakreca-ceny-mieszkan-do-rekordowych-poziomow'
               ]

def get_random_proxy(db): 
    """
    Get a random proxy from the working proxies in the database.

    This function checks the number of active proxies in the 'proxies_working' table of the given database.
    If the number of active proxies is less than 15, it triggers a proxy-checking mechanism by calling the 'check_proxies' function.

    Args:
    db (db_module): An instance of the db_module class providing access to the database.

    Returns:
    str: A randomly selected proxy IP address.

    Raises:
    Exception: If no working proxies are available in the 'proxies_working' table.
    """
    number_of_active_proxies = db.get_row_count("proxies_working")

    if number_of_active_proxies < 15 :
        check_proxies(db)
    
    available_proxies = db.get_column("proxies_working", "ip_address")
    available_proxies = tuple(available_proxies)
    
    if not available_proxies: 
        raise Exception("no proxies available") 
    
    number_of_active_proxies = db.get_row_count("proxies_working")
    print("REMAINING PROXIES: " + str(number_of_active_proxies))  
    info_str =  "REMAINING PROXIES: " + str(number_of_active_proxies)
    logger.info(info_str) 
    return random.choice(available_proxies)



def get(url, proxy: str=None): 
    """
    Perform an HTTP GET request to the specified URL with an optional rotating proxy.

    If no proxy is provided, a random proxy is selected using the 'get_random_proxy' function from the given database.

    Args:
    url (str): The URL to which the GET request should be made.
    proxy (str, optional): The proxy to be used for the request. If not provided, a random proxy will be selected.

    Returns:
    requests.Response: The response object from the GET request.

    Raises:
    Exception: If an exception occurs during the request or if the response status code is not in the VALID_STATUSES.
    """    
    if not proxy: 
        proxy = get_random_proxy(db)   
    user_agent = random.choice(user_agent_list)
    referer = random.choice(referer_list)
    hdr = {'User-Agent': user_agent,
           'Accept-Language': 'pl-PL,pl',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
           'Referer': referer}
    try: 
        # Send proxy requests to the final URL 
        print( "USE PROXY: " + proxy)
        #print("REMAINING PROXIES: " + number_of_active_proxies)
        info_str = "USE PROXY: " + proxy + "\n" + "REMAINING PROXIES: " 
        logger.info(info_str) 
        
        response = requests.get(url, headers=hdr, proxies={'http': f"http://{proxy}"}, timeout=4) 
        print(response.status_code)

        if response.status_code in VALID_STATUSES: # valid proxy 
            set_working(proxy)
            print("RESPONSE STATUS: OK ")
            logger.info("RESPONSE STATUS: OK ")
        else: 
            set_not_working(proxy)
            print("RESPONSE STATUS: FAILED <<<<<<<<<<<<<<<<<< ")
            logger.info("RESPONSE STATUS: FAILED <<<<<<<<<<<<<<<<<< ")
        return response
        
    except Exception as e: 
        print("Exception: ", e)
        set_not_working(proxy)
        print("RESPONSE STATUS: FAILED ")
        logger.info("RESPONSE STATUS: FAILED  ")
   
def check_proxies(db):
    """
    Check the status of proxies in the database and perform checks on a subset of unchecked and not working proxies.

    If the 'proxies_working' table exists and has fewer than 50 active proxies, this function will check a subset of
    unchecked and not working proxies. If the 'proxies_working' table does not exist, it will be created, and checks
    will be performed on unchecked and not working proxies.

    Args:
    db (db_module): An instance of the db_module class providing access to the database.
    """    
    if db.check_table_exists("proxies_working"):
        number_of_active_proxies = db.get_row_count("proxies_working")
        if number_of_active_proxies < 50 :
            print("Now should check some number of  unchecked (and some not working) proxies")
            #Code for checking random proxies
            if not db.is_table_empty("proxies_unchecked"):

                unchecked_id_list = db.get_column("proxies_unchecked", "id")

                if db.get_row_count("proxies_unchecked") > 100: 

                    unchecked_id_list = unchecked_id_list[0:100]

                for proxy_id in unchecked_id_list:

                    proxy = db.get_row_by_id(table_name= "proxies_unchecked", id_value = proxy_id)[1]
                    check_proxy(proxy)

            if not db.is_table_empty("proxies_not_working"):

                if db.get_row_count("proxies_not_working") > 20: 

                    not_working_id_list = db.get_column("proxies_not_working", "id")
                    not_working_id_list = random.choices(not_working_id_list, k=20)

                    for proxy_id in not_working_id_list:

                        proxy = db.get_row_by_id(table_name= "proxies_not_working", id_value = proxy_id)[1]
                        check_proxy(proxy)

        else:
            working_proxies_list  = db.get_column("proxies_working", "ip_address")
    else: 
        print("We have problem! Cant find 'proxies_working' table!")
        db.create_table("proxies_working", "id INTEGER PRIMARY KEY AUTOINCREMENT", "ip_address")

        if not db.is_table_empty("proxies_unchecked"):

            unchecked_id_list = db.get_column("proxies_unchecked", "id")
    
            for proxy_id in unchecked_id_list:

                proxy = db.get_row_by_id(table_name= "proxies_unchecked", id_value = proxy_id)[1]
                check_proxy(proxy)

        elif not db.is_table_empty("proxies_not_working"):
            not_working_id_list = db.get_column("proxies_not_working", "id")
            for proxy_id in not_working_id_list:

                proxy = db.get_row_by_id(table_name= "proxies_not_working", id_value = proxy_id)[1]
                check_proxy(proxy)        
        else:
            raise Exception("Sorry, there's no not_working, unchecked or working proxy. Something went wrong!")

def check_proxy(proxy: str=None): 
    """
    Check the validity of a given proxy by making a test request to a known endpoint.

    Args:
    proxy (str, optional): The proxy to be checked. If not provided, the function will use the 'get_random_proxy' function.

    Raises:
    Exception: If an exception occurs during the request or if the response status code is not in the VALID_STATUSES.
    """    
    print("Sprawdzam proxy: " + proxy)
    info_str = "CHECKING PROXY: " + proxy
    logger.info(info_str)
    get("http://ident.me/", proxy)
       
def reset_proxy(proxy): 
    """
    Reset a proxy by moving it from the 'proxies_not_working' or 'proxies_working' table to the 'proxies_unchecked' table.

    Args:
    proxy (str): The proxy to be reset.

    Note:
    This function removes the specified proxy from both the 'proxies_not_working' and 'proxies_working' tables and inserts
    it into the 'proxies_unchecked' table for further checking.
    """
    db.insert("proxies_unchecked", (None, proxy.strip()))
    db.delete_row(table_name = "proxies_not_working", condition_column = "ip_address", condition_value = proxy)
    db.delete_row(table_name = "proxies_working", condition_column = "ip_address", condition_value = proxy)
    
def set_working(proxy): 
    """
    Set a proxy as working by moving it to the 'proxies_working' table.

    Args:
    proxy (str): The working proxy to be added.

    Note:
    This function inserts the specified working proxy into the 'proxies_working' table and removes it from both the
    'proxies_unchecked' and 'proxies_not_working' tables.
    """
    if not db.check_value_in_column("proxies_working", "ip_address", proxy):
        db.insert("proxies_working", (None, proxy.strip()))
    db.delete_row(table_name = "proxies_unchecked", condition_column = "ip_address", condition_value = proxy)
    db.delete_row(table_name = "proxies_not_working", condition_column = "ip_address", condition_value = proxy)

def set_not_working(proxy): 
    """
    Set a proxy as not working by moving it to the 'proxies_not_working' table.

    Args:
    proxy (str): The not working proxy to be added.

    Note:
    This function inserts the specified not working proxy into the 'proxies_not_working' table and removes it from both
    the 'proxies_unchecked' and 'proxies_working' tables.
    """
    db.insert("proxies_not_working", (None, proxy.strip()))
    db.delete_row(table_name = "proxies_unchecked", condition_column = "ip_address", condition_value = proxy)
    db.delete_row(table_name = "proxies_working", condition_column = "ip_address", condition_value = proxy)


def get_bs_from_url(URL, proxy: str= None):
    """
    Retrieve and parse the HTML content of a given URL using BeautifulSoup.

    This function makes multiple attempts to fetch the content from the specified URL, with an optional rotating proxy.

    Args:
    URL (str): The URL from which to retrieve HTML content.
    proxy (str, optional): The rotating proxy to be used for the request. If not provided, the request will be made without a proxy.

    Returns:
    bs4.BeautifulSoup: A BeautifulSoup object representing the parsed HTML content of the URL.

    Raises:
    NameError: If too many unsuccessful attempts (more than 9) have been made to fetch the content.
    """
    counter = 0
    while True:    
        if counter >= 9: raise NameError('TOO MANY TRIES!!!!')
        try:
            print("Try to get url: " + URL)
            if proxy is not None:
                response = get(URL, proxy)
            else:
                response = get(URL)
            #print(response)
            page = response.text
            bs_from_url = BeautifulSoup(page)

            break
        except Exception as e_2:
            print(e_2) 
        counter +=1
    
    return bs_from_url


def main():

    print("Wykonanie main")

    check_proxies(db) 

    print("unchecked ->", db.get_row_count("proxies_unchecked")) # unchecked -> set() 
    print("working ->", db.get_row_count("proxies_working")) # working -> {"152.0.209.175:8080", ...} 
    print("not_working ->", db.get_row_count("proxies_not_working")) # not_working -> {"167.71.5.83:3128", ...}


    omitted_urls = []
    omitted_urls_exceptions = []

    today_date_str = datetime.today().strftime('%d_%m_%Y')
    file_url = "oto_dom_wroclaw_" + today_date_str 


    with open(file_url, 'w', newline='', encoding="utf-8") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(["titles","prices","location","area","price per square meter","numbers_of_rooms","urls","property_ownership","condition_of_property","floor","balcon_garden_terrace","amount_of_rent","parking_space","type_of_heating","primary_secondary","seller","year_of_construction","type_of_development","window","lift","utilities","security","home_furnishings","additional_info","bulding_material", "describe"])

    #Here modify adress if want scrap from other localization    
    URL = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/dolnoslaskie/wroclaw/wroclaw/wroclaw?limit=36&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing"

    bs = get_bs_from_url(URL)
    page_last_number = int(bs.find_all('a', class_="eo9qioj1 css-5tvc2l edo3iif1")[-1].get_text())
    print("LICZBA STRON DO PRZESZUKANIA: " + str(page_last_number))

    for page_number in range(1, page_last_number + 1):   
        URL_1 = URL + "&page=" + str(page_number)
        bs = get_bs_from_url(URL_1)
        
        for offer in bs.find_all("a", class_="css-1hfdwlm e1dfeild2"):
            offer_url = "https://www.otodom.pl" + offer['href']
        

            try:
                bs_offer = get_bs_from_url(offer_url)
                information_list = []
                
                title = bs_offer.find( class_ = 'css-1wnihf5 efcnut38').get_text()
                price = bs_offer.find( class_ = 'css-t3wmkv e1l1avn10').get_text().replace("\xa0","").replace("zł","").replace(" ","")
                price_per_squered_meter = bs_offer.find( class_ = 'css-1h1l5lm efcnut39').get_text().replace("\xa0","").replace("zł/m²","").replace(" ","")
                
                if price == "Zapytajocenę" and price_per_squered_meter == "":
                    print("Brak ceny, oferta zostanie pominięta")
                    continue
                    
                location = bs_offer.find( class_ = 'css-z9gx1y e3ustps0') 
                if location is not None:
                    location = location.get_text().replace("\xa0","").replace("zł/m²","")       
                else:
                    location = None       
                        
                for information in bs_offer.find_all( class_ = 'enb64yk1'):
                    information_list.append(information.get_text())

                information_list = information_list[1::2]
                apartment_area = information_list[0].replace(" m²","").replace("\xa0","")
                number_of_rooms = information_list[2].replace(" ","")
                property_ownership = information_list[1]
                condition_of_property = information_list[3]
                floor = information_list[4]
                balcon_garden_terrace = information_list[5]
                amount_of_rent = information_list[6]
                parking_space = information_list[7]
                type_of_heating = information_list[9]
                    
                if len(information_list) == 22:
                    primary_secondary = information_list[10]
                    seller = information_list[11]
                    year_of_construction = information_list[13] 
                    type_of_development = information_list[14]
                    window = information_list[15]
                    lift = information_list[16]
                    utilities = information_list[17]
                    security = information_list[18]
                    home_furnishings = information_list[19]
                    additional_info = information_list[20]
                    bulding_material = information_list[21]
                else: 
                    primary_secondary = "brak informacji"
                    seller = "brak informacji"
                    year_of_construction = "brak informacji" 
                    type_of_development = "brak informacji"
                    window = "brak informacji"
                    lift = "brak informacji"
                    utilities = "brak informacji"
                    security = "brak informacji"
                    home_furnishings = "brak informacji"
                    additional_info = "brak informacji"
                    bulding_material = "brak informacji"                
            
                try:
                    describe = bs_offer.find( class_ = 'e175i4j93').get_text().replace("\n"," ").replace("\xa0","").replace("\r"," ").replace("'"," ").replace('"',' ')
                except Exception:
                    describe = ""
  

                row_to_write = [title, price, location, apartment_area, price_per_squered_meter, number_of_rooms, offer_url, property_ownership, condition_of_property, floor, balcon_garden_terrace, amount_of_rent, parking_space, type_of_heating, primary_secondary, seller, year_of_construction, type_of_development, window, lift, utilities, security, home_furnishings, additional_info, bulding_material, describe]
                with open(file_url, 'a', newline='', encoding="utf-8") as file:
                    csv_writer = csv.writer(file)
                    csv_writer.writerow(row_to_write)
                
            except Exception as e_1:
                print(e_1)
                omitted_urls.append(offer_url)
                omitted_urls_exceptions.append(e_1) 

    print(omitted_urls)

if __name__ == '__main__':
    main()
