import pandas as pd
import numpy as np

from rapidfuzz import process
import editdistance

df2 = pd.read_csv('/Users/othman/Downloads/healthFacility_6_9_2022.csv')
df = pd.read_csv('/Users/othman/Downloads/baseline.csv')

hf = df2['HF Name'].unique().tolist()

df['New'] = df['Facilities'].apply(lambda x: ','.join([part for part in hf if part in x]))

df['New'] = df['Facilities'].str.extract(pat = f"({'|'.join(df2['HF Name'])})")[1].map(dict(df2.iloc[:,::-1].values))




cdata  = pd.DataFrame.from_dict(df['Facilities'])
df3 = [df2['HF Name'], df2['Palika']]
headers = ["HF Name", "Palika"]
df4 = pd.concat(df3, axis=1, keys=headers)



clist = pd.DataFrame.from_dict(df4)

# create a choice list
choices = clist['HF Name'].values.tolist()

# apply fuzzywuzzy to each row using lambda expression
cdata['Close HF'] = cdata['Facilities'].apply(lambda x: process.extractOne(x, choices)[0])
cdata[['Close HF', 'Percent Match']] = cdata['Close HF'].apply(pd.Series)


def get_closest(x, column):
    tmp = 1000
    for i2, r2 in clist.iterrows():
        levenshtein = editdistance.eval(x,r2['Country'])
        if levenshtein <= tmp:
            tmp = levenshtein
            res = r2

    return res['BL?']

cdata['BL'] = cdata['Country'].apply(lambda x: get_closest(x, clist))

cdata.to_csv("/Users/othman/Downloads/cdata.csv")

