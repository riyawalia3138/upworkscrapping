#..................................UPWORK-SCRAPPING...................................

from selenium import webdriver
import time
from bs4 import BeautifulSoup
import sqlite3
import multiprocessing
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.proxy import Proxy, ProxyType
import pandas as pd

options = Options()

# sqlite3 database connectivity
conn=sqlite3.connect("up.db") 
                                                
try:
    conn.execute('''CREATE TABLE data(id INTEGER PRIMARY KEY AUTOINCREMENT,skills TEXT)''')
    conn.execute('''CREATE TABLE my_table(id INTEGER PRIMARY KEY AUTOINCREMENT,JobName TEXT,JobDescription LONGTEXT)''')
    print('table create successfully')
except Exception as e:
    print(e)

# get columns data from database 
rows=conn.execute("SELECT DISTINCT up_name FROM upwork")
searchList = []
for row in rows:
    row[0]                                                                  
    searchList.append(row[0])
print('...........list of items............')  
print(searchList) 

findJobLink = []  

print('...........searched item............') 
def iterate(item):
    # driver=webdriver.Chrome()
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    url="https://www.upwork.com/freelance-jobs/"
    driver.get(url)
    driver.maximize_window()
    sPython = driver.find_element(By.LINK_TEXT, item)
    print(sPython.text)    
    time.sleep(0.5)
    findJobLink.append(sPython.get_attribute('href'))   
    data(sPython.get_attribute('href')) 
    
    # driver.close()    
jobdesc =[]
def data(a):
    
    element = []
    # driver=webdriver.Chrome()
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get(a)
    driver.maximize_window()

    soup = BeautifulSoup(driver.page_source, "html.parser")
    for response in soup.select('section[data-qa-section="job-grid"] > div.container > div.air3-grid-container > div > div.job-tile-wrapper > section > div.job-tile-content'):
        
        title = response.a.get('href')
        element.append({
            
             'title': title, 

             })
        
    text = soup.get_text()
    print('text------------>',text)
    print('element------------------>',element)    
    
    
    for x in element:
        # if count == 1:
            # count = count + 1
        x = "https://www.upwork.com" + x['title']
        print("fullLink>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",x)
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        driver.get(x)
        driver.maximize_window()
            # time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        for response in soup.select('div[class="col-12 cfe-ui-job-details-content"] > header'):
            
            JobName = response.h1.get_text(strip=True)
            JobDescription = soup.find('div', { 'class' : 'job-description break mb-0' }).text
            skills = soup.findAll('span', class_=['cfe-ui-job-skill up-skill-badge disabled m-0-left m-0-top m-xs-bottom'])
            # skills = [x.text.replace('\n', '') for x in skills]
            for skill in skills:
                skill = skill.text.replace('\n', '')           
                print(" skills='"+skill+"'")
                skill_count=conn.execute("SELECT COUNT(*) FROM data WHERE skills='"+skill+"'")
                myresult = skill_count.fetchall()
                print(myresult)
                # print(skill_count)
                myresult = myresult[0][0]
                print(myresult)
                if myresult == 0:
                    conn.execute("INSERT INTO data (skills) values(?);",(skill,))                
                    # conn.commit()

            conn.execute('''INSERT INTO my_table (JobName,JobDescription) VALUES(?,?)''',(JobName,JobDescription))

            jobdesc.append({
                
                'JobName':JobName, 
                'JobDescription':JobDescription,
                'skill':skills
                })
        # driver.close()
    print('jobdesc--------------->',jobdesc)
    conn.commit()
    
    data = conn.execute("SELECT * FROM my_table")
    info = data.fetchall()
    print(info)
        
def scrape(url):
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get(url)
    # driver.close()   

processes = []
for url in findJobLink:
    p = multiprocessing.Process(target=scrape, args=(url,))
    p.start()
    processes.append(p)
for p in processes:
    p.join() 

for rec in searchList:
    iterate(rec)
    
print('..........link of searched item...........')        
print(findJobLink) 
conn.close()

df = pd.DataFrame(jobdesc)
print(df)
df.to_csv(r'upwork.csv', sep=',', encoding='utf-8-sig', index=False)
