import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime as dt
import smtplib
import warnings
from sqlalchemy import create_engine

# Ignore all future warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# #DB Details
server="airlineserver1.database.windows.net"
database="airlinedatabase"
username="airlineadmin"
password="Gou@1998"
driver="{SQL Server Native Client 11.0}"

##connection String
connection_string=f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"


#connection Engine
engine=create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")

#all Airline data link
url=fr'https://www.airlinequality.com/review-pages/latest-airline-reviews/'

html_code=requests.get(url=url).content
soup=BeautifulSoup(html_code,'lxml')
box1=soup.find_all('ul',attrs={'class':"item"})

links=[]
try:
    for line in box1:
        name=line.find_all("li")
        for link in name:
            link=link.find("a")['href']
            links.append(fr"https://www.airlinequality.com{link}") #collection of each airline link 
    
    edf=pd.DataFrame()
    for url1 in links:
        page=1
        while(True):
            url=rf"{url1}/page/{page}/?sortby=post_date%3ADesc&pagesize=100"
            list_of_reiviews=[]
        
            html_code=requests.get(url).content
            soup=BeautifulSoup(html_code,'lxml')
            boxes=soup.find_all('article',attrs={'itemprop':"review"})
       
            if(boxes==[]):
                break
            for box in boxes:
                dict_of_air={}
                recorded_date=str(dt.date.today())
                rating=box.find('span',attrs={'itemprop':"ratingValue"})
                if(rating==None):
                    rating=0
                else:
                    rating=rating.get_text()
                title=box.find("h2").get_text().replace('"','')
                review=box.find('div',attrs={'class':'text_content'}).get_text()
                name=box.find("span",attrs={'itemprop':"name"}).get_text()
                flown_date=box.find('time',attrs={'itemprop':'datePublished'})['datetime']
                airline_name=url.split("/")[-5]

        #         ## Table values extraction 
                d={}
                table_values=box.find('table',attrs={'class':"review-ratings"})
                table_rows=table_values.find_all("tr")
                for tr in table_rows:
                    td=tr.find_all("td")
                    key=td[0].get_text()
                    value=td[1]

                    if(value['class']==['review-rating-stars','stars']):
                        value=len(value.find_all("span",'star fill'))
                    else:
                        value=value.get_text()
                    d[key]=value
        #        ## store data in dictionary form
                dict_of_air['Recorded_date']=recorded_date
                dict_of_air["Airline_Name"]=airline_name
                dict_of_air['Rating']=rating
                dict_of_air['Title']=title
                dict_of_air['Name']=name
                dict_of_air["Flown_Date"]=flown_date
                dict_of_air['Review']=review
                dict_of_air["Details"]=d

        #        ## Appen all dict in list to make a json structure        
                list_of_reiviews.append(dict_of_air)
            data=pd.json_normalize(list_of_reiviews)
            data.columns=data.columns.str.replace("&","_").str.replace(" ","_").str.replace("Details.","")
            # data.to_sql(name=f"airlineTable_{airline_name}",schema='dbo',con=engine,if_exists='append')
            
            edf=pd.concat([edf,data]) #two table concatination

            print(f'page-{page} is Collected')
            page+=1
    
#  #data storing in sql database
    edf.to_excel(rf"C:\Users\Goutam\OneDrive\Desktop\Airline_pipeline\Full_Airline_data.xlsx",index=False)
    # edf.to_sql(name="allairlineTable2",schema='dbo',con=engine,if_exists='append')
    # print(edf)
        
except Exception as e:
    email=receiver="gautam.gj31@gmail.com"
    subject=str(dt.date.today())
    massege=f"Something Happend we got error :{e.__doc__}+{str(e)}"

    text=f'Subject: {subject}\n\n{massege}'
    server=smtplib.SMTP("smtp.gmail.com",587)
    server.starttls()
    server.login(email,'gtip lscf ffkw trhz')
    server.sendmail(email,receiver,text)
    print('Error printed please check mail')
