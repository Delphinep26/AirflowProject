import urllib.request
import re
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import urllib
from datetime import datetime
from datetime import date
import time
import random
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float,DateTime,Date
from sqlalchemy import select, desc,exists,and_
from sqlalchemy.sql import func
import os



def extract_data():

    pathDB = 'C:\\AIRFLOW_HOME\\DB'
    #Create DB and Tables 
    engine = create_engine('sqlite:///'+pathDB+'\\superpharmDB.db', echo=False,connect_args={'timeout': 100})
    
    #Connect and loop over the data:
    with engine.connect() as conn:


        #Reset list product errors
        errorsproduct = []

        #Define Browser
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        #Set Main Url
        url = 'http://shop.super-pharm.co.il'
        c = 0
        user_agent = random.choice(user_agent_list)
        headers['User-Agent'] = user_agent
        resp_dep = requests.get(url,headers=headers)
        s_dep = bs(resp_dep.text, 'html.parser')

        #Find All Departments
        dep_rows = s_dep.find_all('li',class_='header-bottom-nav-menu')
        for department in dep_rows:

            d_name = department.a['title']
            d_url = department.a['href']
            time.sleep(random.randint(10, 21))
            user_agent = random.choice(user_agent_list)
            headers['User-Agent'] = user_agent
            resp_cat = requests.get(url+ d_url,headers=headers)
            print(c)
            
            s_categories = bs(resp_cat.text, 'html.parser')

            #Find All Category by Department
            categories_rows = s_categories.find_all('p' , class_ = ['phcCatImage','phcCategoryImage',' phcCategoriesImage'])

            #Except some departments
            if d_url[1:] in ['baby','elder','medical-accessories','nature','pharmacy','promotions','coupons']:
                continue
                
            print("Department:", d_name)

            for c_num , category in enumerate(categories_rows):
                try:
                    c_name = category.a['title']
                    print("Category:", c_name) 
                    c_url = category.a['href']

                    #Except dermocosmetics category
                    if c_url == '/dermocosmetics' or c_url == 'https://optic-express.co.il/16-toric':
                        continue
                    resp_item = requests.get(url + c_url)  

                    s_items = bs(resp_item.text, 'html.parser')
                    #print(url+ c_url)

                    #Find All Products by Category
                    products = s_items.find('div',class_ ='clearfix boxes-wrap').find_all('a')

                    for i,product in enumerate(products):

                        try:
                            prod_url = url + product.get('href')
                            prod_item_box = product.find(class_ = "item-box")
                            prod_id = prod_item_box['data-id']
                            prod_brand = prod_item_box.h4.text
                            prod_name= prod_item_box.p.text
                            prod_image_url = prod_item_box.find(class_ ='item-image').img['src']
                            s_desc = prod_item_box.find(class_ = 'description-wrap')
                            if s_desc != None:
                                prod_desc = s_desc.span.text
                            else:
                                prod_desc = ''

                            priceclass = prod_item_box.find(class_ = 'item-price special-price')
                            if priceclass is None :
                                priceclass = product.find(class_ = "item-box").find(class_ = 'item-price no-special') 
                                p_shekels = priceclass.find(class_='shekels money-sign').text
                                #print("price:",p_shekels)
                                p_cents = priceclass.find(class_='cents').text
                            else:
                                p_shekels = priceclass.find_all(class_='shekels money-sign')[1].text
                                #print("else:",p_shekels)
                                p_cents = priceclass.find_all(class_='cents')[1].text

                            prod_price = float((p_shekels.replace(',','')) + '.' + p_cents) 

                            #Insert to DB:
                            s = items.select().where(items.c.id == prod_id)
                            s = exists(s).select()

                            result = conn.execute(s).scalar()

                            if not result :

                                try:

                                    ins = items.insert().values(id=prod_id, 
                                                                name=prod_name,
                                                                description=prod_desc,
                                                                department=d_name, 
                                                                category=c_name, 
                                                                brand=prod_brand, 
                                                                url=prod_url, 
                                                                image_url=prod_image_url,
                                                                update_date=date.today())

                                    result = conn.execute(ins)
                                    print("Item :",prod_id , "inserted" ,prod_image_url )
                                    break

                                except Exception as e:

                                    errorsproduct.append(e)

                                    print("Error in inserting ", i , "Product from" ,c_num, c_name , 'Category')




                            s = prices.select().where(and_(prices.c.date == date.today(), prices.c.item_id == prod_id))
                            s = exists(s).select()
                            result = conn.execute(s).scalar()

                            if not result :

                                ins = prices.insert().values(item_id=prod_id, \
                                                            amount=prod_price, \
                                                            date=date.today())


                                result = conn.execute(ins)
                                print("Item :",prod_id , "Price inserted",prod_price,prod_image_url )           

                        except Exception as e:
                            
                            errorsproduct.append(product)
                            print("Error in extract ", i , "Product from" ,c_num, c_name , 'Category')


                    time.sleep(random.randint(10, 21))
                    user_agent = random.choice(user_agent_list)
                    headers['User-Agent'] = user_agent
                    
                except Exception as e:

                                    errorsproduct.append(e)

                                    print("Error in Extracting ", c_num , "Category from" ,c_name , 'Category')

