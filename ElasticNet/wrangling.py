import pandas as pd
import glob
import re
import datetime

li = []

for filename in glob.glob("./ElasticNet/data/*.csv"):
    df = pd.read_csv(filename, index_col=None, header=0)
    df["year"] = [int(re.findall(r'\d+', filename)[0])] * len(df)
    li.append(df)
frame = pd.concat(li, axis=0, ignore_index=True)

frame['year_mon'] = frame.apply(lambda x: datetime.date(datetime.datetime.strptime(x.Month, "%B").month, x.year, 1),
                                axis=1)
frame['mon_int'] = frame.Month.apply(lambda x: datetime.datetime.strptime(x, "%B").month)

a = frame.groupby(['year', 'Month'])['RentalCarGrossSales'].agg(['sum', 'count'])
