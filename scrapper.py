import requests, bs4, sys, csv, datetime

now = datetime.datetime.now()

#the scraper axcepts two command line arguments - maker and model
#TO DO: validate input, apparently argparse is helpful
maker = sys.argv[1]
model = sys.argv[2]

PATH = 'https://www.otomoto.pl/osobowe/citroen/c5/rawa-mazowiecka/?search%5Bfilter_enum_fuel_type%5D%5B0%5D=petrol&search%5Bfilter_enum_fuel_type%5D%5B1%5D=petrol-lpg&search%5Border%5D=created_at_first%3Adesc&search%5Bbrand_program_id%5D%5B0%5D=&search%5Bdist%5D=155&search%5Bcountry%5D='
FILE_NAME = maker + '-' + model + '-' + str(now.date()) + '.csv'


def get_html_page_as_bs(link):
    res = requests.get(PATH)
    res.raise_for_status()
    return bs4.BeautifulSoup(res.text, features="lxml")

def get_car_list():
    return currentPage.select('article.offer-item')


#prepare the file
# carFile = open(FILE_NAME, 'w', newline="")
# outputWriter = csv.writer(carFile)

carSoup = get_html_page_as_bs(PATH)

lastPage = int(carSoup.select('.page')[-1].text)

for link in carSoup.find_all('a'):
            print(link.get('href'))

for i in range(1):
    res = requests.get(PATH + '?page=' + str(i))
    res.raise_for_status()
    currentPage = bs4.BeautifulSoup(res.text, features='lxml')
    carList = get_car_list()
    print("parsing page " + str(i))
    for car in carList:
        #get the interesting data and write to file
        currentCarData = []
        price = car.find('span',class_='offer-price__number').text.strip().replace(" ", "")
        currentCarData.append(price)
        title = car.find('a',class_='offer-title__link').text.strip()
        currentCarData.append(title)

        # get car 
        PATH = car.find_all('a')[0]   

        #Iterate through parameters
        paramList = ["year", "mileage", "engine_capacity", "fuel_type"]
        print(type(car.find('offer-item__title')))
        for param in paramList:
            currentParameter = car.find('li', {"data-code": param})
            if (currentParameter):
                currentCarData.append(currentParameter.text.strip())
                print(currentParameter.text.strip())
            else:
                currentCarData.append("")

        outputWriter.writerow(currentCarData)

carFile.close()
