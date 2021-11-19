import requests
import lxml.html as lh
import pandas as pd

pd.set_option("display.max_columns",None)

year = 2021
url='https://www.portseattle.org/page/rental-car-revenue-report-2018'
#Create a handle, page, to handle the contents of the website
page = requests.get(url)
#Store the contents of the website under doc
doc = lh.fromstring(page.content)
#Parse data that are stored between <tr>..</tr> of HTML
tr_elements = doc.xpath('//tr')

#Check the length of the first 12 rows
[len(T) for T in tr_elements[:12]]


#Create empty list
col=[]
i=0
#For each row, store each first element (header) and an empty list
for t in tr_elements[0]:
    i+=1
    name=t.text_content()
    print('%d:"%s"'%(i,name))
    col.append((name,[]))


# Since out first row is the header, data is stored on the second row onwards
for j in range(1, len(tr_elements)):
    # T is our j'th row
    T = tr_elements[j]

    # If row is not of size 10, the //tr data is not from our table
    if len(T) > 7:
        break

    # i is the index of our column
    i = 0

    # Iterate through each element of the row
    for t in T.iterchildren():
        data = t.text_content()
        # Check if row is empty
        if i > 0:
            # Convert any numerical value to integers
            try:
                data = int(data)
            except:
                pass
        # Append the data to the empty list of the i'th column
        col[i][1].append(data)
        # Increment i for the next column
        i += 1

[len(C) for (title,C) in col]

Dict = {title: column for (title,column) in col[:-1]} # why the last col doesn't have enough row?
df = pd.DataFrame(Dict)

import re
# Data cleaning
df.columns = [(re.sub('\s+', '',c.replace(u'\xa0', u' '))).strip() for c in df.columns]
df = df[~df.Month.str.contains('Total')]

for _,row in df.iterrows():
    if row.Month != '\xa0':
        tmp_mon = row.Month
        print(tmp_mon)
    df.loc[_,'Month'] = tmp_mon

df.Company = df.Company.apply(lambda x: x.replace(u'\xa0', u' '))
df.RentalCarGrossSales = df.RentalCarGrossSales.apply(lambda x: int(str(x).replace(',','')))
df.to_csv(f'./data/rental{year}.csv')