import requests, bs4, sys, csv, datetime

now = datetime.datetime.now()

#the scraper axcepts two command line arguments - maker and model
#TO DO: validate input, apparently argparse is helpful
# maker = sys.argv[1]
# model = sys.argv[2]

# variables
# html_page - whole html page from PATH 
MAKER = 'opel'
MODEL = 'astra'
PATH = 'https://www.otomoto.pl/osobowe/citroen/c5/rawa-mazowiecka/?search%5Bfilter_enum_fuel_type%5D%5B0%5D=petrol&search%5Bfilter_enum_fuel_type%5D%5B1%5D=petrol-lpg&search%5Border%5D=created_at_first%3Adesc&search%5Bbrand_program_id%5D%5B0%5D=&search%5Bdist%5D=155&search%5Bcountry%5D='
FILE_NAME = MAKER + '-' + MODEL + '-' + str(now.date()) + '.csv'
carFile = open(FILE_NAME, 'w', newline="")
outputWriter = csv.writer(carFile)

def get_html_page_as_bs(link):
    res = requests.get(link)
    res.raise_for_status()
    return bs4.BeautifulSoup(res.text, features="lxml")

def get_car_item_list(html_page):
    return html_page.select('article.offer-item')
    
def get_number_of_pages(html_page):
    return int(html_page.select('.page')[-1].text)    

def get_link_to_car_item(item):
    return item.find_all('a')[0].get('href')

def get_item_price(item):
    return item.find('span',class_='offer-price__number').text.strip().replace(" ", "")

def get_item_title(item):
    return item.find('a',class_='offer-title__link').text.strip()

def get



html_page = get_html_page_as_bs(PATH)
pages_number = get_number_of_pages(html_page)


for i in range(1,pages_number):

    current_page = get_html_page_as_bs(PATH + '?page=' + str(i))
    car_item_list = get_car_item_list(current_page)

    for item in car_item_list:
        #get the interesting data and write to file
        current_car_data = []

        current_car_data.append(get_item_price(item))

        current_car_data.append(get_item_title(item))

        link_to_car = get_link_to_car_item(item)
        current_car_data.append(link_to_car)

  
        #Iterate through parameters
        paramList = ["year", "mileage", "engine_capacity", "fuel_type"]

        for param in paramList:
            currentParameter = item.find('li', {"data-code": param})
            if (currentParameter):
                current_car_data.append(currentParameter.text.strip())
                # print(currentParameter.text.strip())
            else:
                current_car_data.append("")

        # go deeper into item page(for each car in list)
        # get 


        print(current_car_data)
        outputWriter.writerow(current_car_data)

carFile.close()
