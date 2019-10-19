import urllib.request
from bs4 import BeautifulSoup
from currency_converter import CurrencyConverter
import pymongo
import lxml
from tqdm import tqdm

def get_html(url):
    response = urllib.request.urlopen(url) #getting html code
    return response.read()


def parseGender(stats):   
    clusters = stats[11].find_all('span',class_='clusters-value__count')
    genders = []
    genders.append(
        {"Мужской": int(clusters[0].text), "Женский": int(clusters[1].text)}
        )
    return genders
def parseAges(stats):   
    clusters = stats[7].find_all('span',class_='clusters-value__count')
    ages = []
    ages.append(
        {"14-18": int(clusters[0].text), "18-30": int(clusters[1].text), "30-40": int(clusters[2].text),
        "40-50": int(clusters[3].text), "50-60": int(clusters[4].text), "60+": int(clusters[5].text)}
        )
    return ages
def parseAreas(stats):
    
    clusters = stats[1].find_all('span',class_='clusters-value__count')
    clusterName = stats[1].find_all('span',class_='clusters-value__name')
    areas = {}
    for i in range(0,len(clusterName)):
        areas.update(
            {str(clusterName[i].text).replace('.','истративный'): int(clusters[i].text)})
    return areas
def parseSkills(stats):    
    clusters = stats[4].find_all('span',class_='clusters-value__count')
    clusterName = stats[4].find_all('span',class_='clusters-value__name')
    skills = {}
    for i in range(0,len(clusterName)):
        skills.update(
            {str(clusterName[i].text): int(clusters[i].text)})
    return skills

def ExportToDB(collection,items):
    mongo = MongoConnect() #connect to the server
    hh_ru = mongo.hh_ru    #connect to the DB
    col = hh_ru[collection]    #connect to the collection
    col.drop()     #clear collection
    with tqdm(total = len(items)) as pbar:
        for item in items:     #add resumes as documents to the collection
            col.insert_one(item).inserted_id
            pbar.update(1)

def MongoConnect():
    url = 'mongourl'
    dbs = pymongo.MongoClient(
        url,
        username='username',
        password='password')
    return dbs

def main():    #all resumes cant place in one url so split it to three
   url = "https://novosibirsk.hh.ru/search/resume?text=&st=resumeSearch&logic=normal&pos=full_text&exp_period=all_time&area=4&relocation=living_or_relocation&salary_from=&salary_to=&currency_code=RUR&label=only_with_salary&education=none&university=40104&age_from=&age_to=&gender=unknown&order_by=publication_time&search_period=0&items_on_page=20"
   soup = BeautifulSoup(get_html(url), features = "html.parser")    
   table = soup.find('div', class_='bloko-column bloko-column_l-3 bloko-column_m-3')
   stats = table.find_all('div',class_= 'clusters-group clusters-group_expand')
   genders = parseGender(stats)
   ExportToDB('genders',genders)
   ages = parseAges(stats)
   ExportToDB('ages',ages)
   areas = [parseAreas(stats)]
   ExportToDB('areas',areas)
   skills = [parseSkills(stats)]
   ExportToDB('skills',skills)

main()
