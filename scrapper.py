import requests, bs4, sys, csv, datetime
import pandas as pd
import time
import concurrent.futures
import traceback 
import math

CORES_NUMBER = 8

start_time = time.time()
rows_list = []

# PATH = r'https://www.otomoto.pl/osobowe/opel/astra/?search%5Border%5D=created_at_first%3Adesc&search%5Bbrand_program_id%5D%5B0%5D=&search%5Bcountry%5D=0%3Fpage%3D3&page='
PATH = r'https://www.otomoto.pl/osobowe/opel/astra/?search%5Border%5D=created_at_first%3Adesc&search%5Bbrand_program_id%5D%5B0%5D=&search%5Bcountry%5D=0'
EXPECTED_LABELS = ['Oferta od', 'Kategoria', 'Wersja','Rok produkcji', 'Przebieg', 'Pojemność skokowa', 'Rodzaj paliwa', 'Moc', 'Skrzynia biegów', 'Typ', 'Kraj pochodzenia','Generacja']


def get_html_page_as_bs(link):
    '''
    Returns the page as BeautifulSoup object.

            Parameters:
                    link (string): A string link to page

            Returns:
                    BeautifulSoup (object): page as BeautifulSoup object
    '''

    res = requests.get(link)
    res.raise_for_status()
    return bs4.BeautifulSoup(res.text, features="lxml")

def get_car_item_list(html_page):
    """Takes in BeautifulSoup object with car advertisement list page , returns car items as list."""
    return html_page.select('article.offer-item')
    
def get_number_of_pages(html_page):
    """Takes in BeautifulSoup object with car advertisement list page, returns pages number as integer."""
    return int(html_page.select('.page')[-1].text)    

def get_link_to_car_item(item):
    """Takes in BeautifulSoup object with specific car advertisement page, returns car item as list."""
    return item.find_all('a')[0].get('href')

def get_item_price(item):
    """Takes in BeautifulSoup object with specific car advertisement page, returns car price."""
    return item.find('span',class_='offer-price__number').text.strip().replace(" ", "")

def get_item_title(item):
    """Takes in BeautifulSoup object with specific car advertisement page, returns car title."""
    return item.find('a',class_='offer-title__link').text.strip()

def append_car_info_to_data_frame(item_html_page,rows_list):
    '''
    Extracts data from car advertisement page and assigns to global list.

            Parameters:
                    item_html_page(BeautifulSoup): car advertisement page
                    rows_list (list): contains all extracted data as list of dicts
           
    '''
    ul = item_html_page.find_all('li', {'class': 'offer-params__item'})
    dict_all = {}

    # go through li tagas and find expexced labels
    for li in ul:
        label = li.find('span', {'class':'offer-params__label'}).text
        if label not in EXPECTED_LABELS and label != "Generacja" :
            continue

        value = li.find('div', {'class':'offer-params__value'}).find('a')

        if value:
            value = value.text
        else:
            value = li.find('div', {'class':'offer-params__value'}).text
        
        dict_all.update({label:value.strip()}) 
    
    rows_list.append(dict_all)

def get_number_of_pages_per_process(pages_number):
    """Takes in pages number, returns dict with caclulates number of pages per each process."""
    if pages_number < CORES_NUMBER:
        return {"pages_per_process": 1,"pages_to_last_process": 0}

    pages_per_process = math.ceil(pages_number/CORES_NUMBER)
    pages_to_last_process = pages_per_process - ((pages_per_process * CORES_NUMBER) - pages_number) 
    return {"pages_per_process": pages_per_process , "pages_to_last_process": pages_to_last_process}

def get_data(http_urls):
    """Takes in list of urls to process, returns list with extacted data."""
    rows_list = []

    # using threadPool, load car advertisement list pages
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(get_html_page_as_bs,url ): url for url in http_urls}
            print("ss")
            for index,future in enumerate(concurrent.futures.as_completed(future_to_url)):
                url = future_to_url[future]
                
                print(f'///////////////////////////////////////////////////////////////////||||{index}')
                try:
                    data = None
                    print("first_loop")
                    data = future.result()
                    car_item_list = get_car_item_list(data)
                    item_http_link = [get_link_to_car_item(item) for item in car_item_list]

                    # using threadPool, load each car advertisement page
                    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as e:
                        # Start the load operations and mark each future with its URL
                        to_url = {e.submit(get_html_page_as_bs,url): url for url in item_http_link}
                        for f in concurrent.futures.as_completed(to_url):
                            try:
                                print("second_loop")
                                item = f.result()

                                # using threadPool, call method append_car_info_to_data_frame
                                with concurrent.futures.ThreadPoolExecutor(max_workers=30) as e:
                                    e.submit(append_car_info_to_data_frame,item,rows_list)
                        
                            except Exception as exc:
                                traceback.print_exc() 
                                print('%r generated an exception: %s' % (url, exc))
                          
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
    return rows_list


if __name__ == '__main__':       
    end_list = []
    html_page = get_html_page_as_bs(PATH)
    pages_number = get_number_of_pages(html_page)
    # http_urls = [PATH +"%3Fpage%3D3&page="+ str(i) for i in range(1,pages_number)]
    http_urls = [PATH +"%3Fpage%3D3&page="+ str(i) for i in range(1,pages_number)]

    # create processes depends on number of pages to process
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = []
        start = 1
        pages_per_process = get_number_of_pages_per_process(pages_number)

        for stop in range(pages_per_process['pages_per_process'],pages_number + 1,pages_per_process['pages_per_process']):
            results.append(executor.submit(get_data,http_urls[start:stop]))
            start = stop + 1

        if pages_per_process['pages_to_last_process'] != 0:
            results.append(executor.submit(get_data,http_urls[start: start+pages_per_process['pages_to_last_process']]))

        for future in concurrent.futures.as_completed(results):
            end_list = end_list + future.result()

    df = pd.DataFrame(end_list) 
            
    df.to_csv('car_info_data_frame.csv', sep='\t', encoding='utf-8',index=False)

    print("--- %s seconds ---" % (time.time() - start_time))