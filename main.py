
import nltk
from nltk.tokenize import MWETokenizer
import sqlalchemy as db
from sqlalchemy.sql import text
import pandas as pd
import re
import string

articles = pd.read_csv("articles.csv")
bills = pd.read_csv("bills.csv")
hierarchy = pd.read_csv("hierarchy.csv")

#creating a connection to MySQL Engine user:root, password:root and database:bott
engine = db.create_engine('mysql://root:root@localhost/bott')
connection = engine.connect()
metadata = db.MetaData()

#convering Dfs to tables
bills.to_sql('bills', con=engine)
articles.to_sql('articles', con=engine)
hierarchy.to_sql('hier', con=engine)
nltk.download('punkt')

#creating a list of category, sub category and brand names
categoryname = hierarchy.category_name.unique()
sub_category = hierarchy.subcategory_name.unique()
brand = hierarchy.brand_name.unique()
article=articles.name.unique()

#A function that gets the category, sub category or brand name and the dates of bills
def getcategory(tokens):
  flag=0
  for word in tokens:
    if word in categoryname:
      cat = word
      cate = 'category'
      break
    elif word in sub_category:
      cat = word
      cate = 'subcategory'
      break
    elif word in brand:
      cat = word
      cate = 'brand'
      break
    else:
      cat=" "
      cate=" "
  if not cat and not cate: return
  
  
  r = re.compile('[0-1]*[0-9]\/[0-3]*[0-9]\/[0-9]{4}')
  beg_date=''
  end_date=''
  for date in tokens:
    if r.match(date) and flag ==0:
      flag = 1
      beg_date=date
    elif r.match(date) and flag ==1:
      flag = 2
      end_date=date
      categorize(cate,cat,beg_date,end_date)
      break
  
  if flag==1:
    categorizei(cate,cat,beg_date)
  else:
    for month in tokens:
      if month=='july':
        beg_date='7/1/2018'
        end_date='7/31/2018'
      elif month=='august':
        beg_date='8/1/2018'
        end_date='8/31/2018'
    categorize(cate,cat,beg_date,end_date)
  if not beg_date and  not end_date: return


#A function that extracts data from the DB and prints the result
def categorize(category,cat,beg_date,end_date):
  if category=='category':
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select hier.article_id from hier where hier.category_name = :x) and sale_date between :y and :z;")
    res=engine.execute(s, {"x":cat,"y":beg_date,"z":end_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)
  elif category=='subcategory':
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select hier.article_id from hier where hier.subcategory_name = :x) and sale_date between :y and :z;")
    res=engine.execute(s, {"x":cat,"y":beg_date,"z":end_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)
  elif category=='brand':
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select hier.article_id from hier where hier.brand_name = :x) and sale_date between :y and :z;")
    res=engine.execute(s, {"x":cat,"y":beg_date,"z":end_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)
  else:
    s = text("select sum(bills.total_price) from bills where sale_date between :y and :z;")
    res=engine.execute(s, {"y":beg_date,"z":end_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)

#A function that extracts data from the DB and prints the result but for a particular day
def categorizei(category,cat,beg_date):
  if category=='category':
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select hier.article_id from hier where hier.category_name = :x) and sale_date = :y;")
    res=engine.execute(s, {"x":cat,"y":beg_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)
  elif category=='subcategory':
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select hier.article_id from hier where hier.subcategory_name = :x) and sale_date = :y;")
    res=engine.execute(s, {"x":cat,"y":beg_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)
  elif category=='brand':
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select hier.article_id from hier where hier.brand_name = :x) and sale_date = :y;")
    res=engine.execute(s, {"x":cat,"y":beg_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)
  else:
    s = text("select sum(bills.total_price) from bills where sale_date = :y;")
    res=engine.execute(s, {"y":beg_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    print('Rs. '+ans)


#A function that segregates the date for top selling articles/brand/category
def gettop(tokens):
  flag = 0
  for word in tokens:
    if (word == 'category')or(word=='subcategory') or (word=='brand')or(word=='article'):
      cat=word
  if not cat: return
  
  r = re.compile('[0-1]*[0-9]\/[0-3]*[0-9]\/[0-9]{4}')
  beg_date=''
  end_date=''
  for date in tokens:
    if r.match(date) and flag ==0:
      flag = 1
      beg_date=date
    elif r.match(date) and flag ==1:
      flag = 2
      end_date=date
      topsale(cat,beg_date,end_date)
      break
  
  
  if flag==1:
    topsalei(cat,beg_date)
  else:
    for month in tokens:
      if month=='july':
        beg_date='7/1/2018'
        end_date='7/31/2018'
      elif month=='august':
         beg_date='8/1/2018'
         end_date='8/31/2018'
    topsale(cat,beg_date,end_date)
  
  if not beg_date and  not end_date: return


#A function that executes the DB query according to gettop()
def topsale(category,beg_date,end_date):
  categorylist=[]
  sumlist=[]
  
  def execute(s,cat,beg_date,end_date):
    res=engine.execute(s, {"x":cat,"y":beg_date,"z":end_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    if(ans!='None'):
      sumlist.append(float(ans))
      categorylist.append(cat)
  
  
  if category=='category':
    for cat in categoryname:
      s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,hier where bills.article_id = hier.article_id and hier.category_name= :x) and bills.sale_date between :y and :z;")
      execute(s,cat,beg_date,end_date)
    print("Category: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist)))
  
  elif category=='subcategory':
    for cat in sub_category:
      s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,hier where bills.article_id = hier.article_id and hier.subcategory_name= :x) and bills.sale_date between :y and :z;")
      execute(s,cat,beg_date,end_date)
    print("Subcategory: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist)))
  
  elif category=='brand':
   for cat in brand:
     s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,hier where bills.article_id = hier.article_id and hier.brand_name= :x) and bills.sale_date between :y and :z;")
     execute(s,cat,beg_date,end_date)
   print("Brand: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist)))
  elif category=='article':
    for cat in article:
     s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,articles where bills.article_id = articles.id and articles.name= :x) and bills.sale_date between :y and :z;")
     execute(s,cat,beg_date,end_date)
    for i in range(95):
     if articles.values[i,1]==cat:
       artid=articles.values[i,0]
       break
    print("ID: "+str(artid)+" Article: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist))) 
  
  if len(sumlist)==0: return

#A function that executes the DB query according to gettop() but for a particular day
def topsalei(category,beg_date):
  categorylist=[]
  sumlist=[]
  
  def execute(s,cat,beg_date):
    res=engine.execute(s, {"x":cat,"y":beg_date}).fetchall()
    ans = re.sub('[\[,\(\)\]]', '', str(res[0]))
    if(ans!='None'):
      sumlist.append(float(ans))
      categorylist.append(cat)
  
  if category=='category':
    for cat in categoryname:
      s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,hier where bills.article_id = hier.article_id and hier.category_name= :x) and bills.sale_date= :y;")
      execute(s,cat,beg_date)
    print("Brand: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist)))
  elif category=='subcategory':
    for cat in sub_category:
      s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,hier where bills.article_id = hier.article_id and hier.subcategory_name= :x) and bills.sale_date=:y;")
      execute(s,cat,beg_date)
    print("Brand: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist)))
  elif category=='brand':
   for cat in brand:
     s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,hier where bills.article_id = hier.article_id and hier.brand_name= :x) and bills.sale_date=:y;")
     execute(s,cat,beg_date)
   print ("Brand: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist)))
  elif category=='article':
   for cat in article:
    s = text("select sum(bills.total_price) from bills where bills.article_id in (select distinct bills.article_id from bills,articles where bills.article_id = articles.id and articles.name= :x) and bills.sale_date = :y;")
    execute(s,cat,beg_date)
   for i in range(95):
       if articles.values[i,1]==cat:
         artid=articles.values[i,0]
         break
   print("ID: "+str(artid)+" Article: "+categorylist[sumlist.index(max(sumlist))] +" SalesValue: "+str(max(sumlist))) 
  
  if len(sumlist)==0: return

#A function that seperates input string into tokens
def Tokenize(text):
  tokenizer = MWETokenizer(category.all())
  for word in category:
    if word.find(' '): 
      tokenizer.add_mwe(word.split())
  for word in sub_category:
     if word.find(' '): 
       tokenizer.add_mwe(word.split())
  for word in brand:
     if word.find(' '): 
       tokenizer.add_mwe(word.split())
  for word in article:
     if word.find(' '): 
       tokenizer.add_mwe(word.split())
  
  token=tokenizer.tokenize(text.split())
  tokens=[]
  for word in token:
    word=word.replace("_"," ")
    tokens.append(word)
  return tokens


def main():
  text = str(input("Enter query\n"))
  text=text.lower()
  tokens=Tokenize(text)
  for word in tokens:
    if word == 'sales':
      getcategory(tokens)
    elif word == 'top':
      print("in top")
      gettop(tokens)
    else: return



if __name__ == '__main__':
  main()