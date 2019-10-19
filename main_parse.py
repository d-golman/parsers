import urllib.request
from bs4 import BeautifulSoup
from currency_converter import CurrencyConverter
import pymongo
import lxml
from tqdm import tqdm

def get_html(url):
    response = urllib.request.urlopen(url) #getting html code
    return response.read()

def get_pages(html):
    soup = BeautifulSoup(html, features = "html.parser")
    paggination = soup.find_all('a', class_='bloko-button HH-Pager-Control')[-1]    #find amount of pages in html
    number = int(paggination.text) - 1 
    return number

def parse(html):    
    soup = BeautifulSoup(html, features = "html.parser")    
    table = soup.find('div', class_='bloko-column bloko-column_l-13 bloko-column_m-9')  #find list of resumes in html
    resumes = table.find_all('div',itemtype= 'http://schema.org/Person')    #find separate resumes in list
    resumeList = []
    for resume in resumes:     #find needed information in resumes
        if resume.find('a', class_='resume-search-item__name HH-VisitedResume-Href HH-LinkModifier'):
            occupation = resume.find('a', class_='resume-search-item__name HH-VisitedResume-Href HH-LinkModifier').text
        else:
            occupation = None
        if resume.find('div', class_='resume-search-item__fullname'):
            if len(resume.find('div', class_='resume-search-item__fullname').span.text.split('\xa0',1)) == 2:
                age, trash = resume.find('div', class_='resume-search-item__fullname').span.text.split('\xa0',1)
                age = int(age)
            else:
                age = None
        else:
            age = None
        if len(resume.find('div', class_='resume-search-item__compensation').text) > 0:
            if (resume.find('div', class_='resume-search-item__compensation').text)[len(resume.find('div', class_='resume-search-item__compensation').text)-3:len(resume.find('div', class_='resume-search-item__compensation').text)] == "USD":
                presalary, trash = resume.find('div', class_='resume-search-item__compensation').text.split('\xa0',1)
                salary = CurrencyConverter().convert(float(presalary),'USD','RUB')			
            else:
                if (resume.find('div', class_='resume-search-item__compensation').text)[len(resume.find('div', class_='resume-search-item__compensation').text)-3:len(resume.find('div', class_='resume-search-item__compensation').text)] == "EUR":
                    presalary, trash = resume.find('div', class_='resume-search-item__compensation').text.split('\xa0',1)
                    salary = CurrencyConverter().convert(float(presalary),'EUR','RUB')
                else:
                    presalary, trash = resume.find('div', class_='resume-search-item__compensation').text.split('\xa0',1)
                    salary = float(presalary)
        else:
            salary = ''
        if 	resume.find('div', class_='resume-search-item__description-content'):
            preexp = resume.find_all('div', class_='resume-search-item__description-content')[0].text
            if 15 < len(preexp) > 0 and len(preexp.split('\xa0',1)) != 1:
                preexp, trash = preexp.split('\xa0',1)
                exp = float(preexp)
                exp = exp + 0.5
            else:
                exp = 0
        else:
            exp = 0		
        if resume.find('span', class_='resume-search-item__company-name'):
            lastJob = resume.find('span', class_='resume-search-item__company-name').text
        else:
            lastJob = None
        resumeList.append({    #add collected information to the list
            'occupation': occupation,
            'age': age,
            'salary': salary,
			'exp': exp,
			'lastJob': lastJob
        })
    return resumeList

def ExportToDB(items):
    mongo = MongoConnect() #connect to the server
    hh_ru = mongo.hh_ru    #connect to the DB
    resumes = hh_ru.resumes    #connect to the collection
    resumes.drop()     #clear collection
    with tqdm(total = len(items)) as pbar:
        for item in items:     #add resumes as documents to the collection
            resumes.insert_one(item).inserted_id
            pbar.update(1)

def MongoConnect():
    url = 'mongourl'
    dbs = pymongo.MongoClient(
        url,
        username='username',
        password='password')
    return dbs

def main():    #all resumes cant place in one url so split it to three
    urls = ['https://novosibirsk.hh.ru/search/resume?L_is_autosearch=false&area=4&clusters=true&exp_period=all_time&items_on_page=100&label=only_with_salary&logic=normal&no_magic=false&order_by=publication_time&pos=full_text&salary_from=0&salary_to=27000&text=&university=40104&page=0',
			'https://novosibirsk.hh.ru/search/resume?L_is_autosearch=false&area=4&clusters=true&exp_period=all_time&items_on_page=100&label=only_with_salary&logic=normal&no_magic=false&order_by=publication_time&pos=full_text&salary_from=27001&salary_to=45000&text=&university=40104&page=0',
			'https://novosibirsk.hh.ru/search/resume?L_is_autosearch=false&area=4&clusters=true&exp_period=all_time&items_on_page=100&label=only_with_salary&logic=normal&no_magic=false&order_by=publication_time&pos=full_text&salary_from=45001&text=&university=40104&page=0']			
    items = []
    for url in urls:
        total_pages = get_pages(get_html(url))
        with tqdm(total = total_pages) as pbar:
            for page in range(0, total_pages):     #parsing each page of each url
                removal = url.split('=')[-1]
                reverse_removal = removal[::-1]
                replacement = str(page)
                reverse_replacement = replacement[::-1]
                items.extend(parse(get_html(url[::-1].replace(reverse_removal, reverse_replacement, 1)[::-1])))
                pbar.update(1)
    ExportToDB(items)

main()
