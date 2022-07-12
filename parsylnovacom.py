import pandas as pd
import numpy as np
import datetime


df1 = pd.read_csv('/Users/othman/Downloads/Sénégal RM de Saint-Louis, DEPÔT MOBILE TOYOTA LAND-CRUISER_Trek 64-918 (1).csv')

df1.rename(columns={'time (UTC)':'time'},inplace=True)

df1.time  = pd.to_datetime(df1.time)

df1['time']= df1['time'].apply(lambda x: x.strftime("%d/%m/%Y %H:%M"))

df1.to_csv("/Users/othman/Downloads/parslysenegal.csv")

import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

df2 = pd.read_csv('/Users/othman/Downloads/exportto_csv.csv')

x = df2['time']
   # [dt.datetime(2009, 05, 01), dt.datetime(2010, 06, 01),
    # dt.datetime(2011, 04, 01), dt.datetime(2012, 06, 01)]
y = df2['temp (C)']

fig, ax = plt.subplots()
ax.plot_date(x, y, linestyle='--')

ax.annotate('Test', (mdates.date2num(x[1]), y[1]), xytext=(15, 15),
            textcoords='offset points', arrowprops=dict(arrowstyle='-|>'))

fig.autofmt_xdate()
plt.show()

df3 = pd.read_csv('/Users/othman/Downloads/export (3) - Sheet3.csv')

BBox = ((df3.long.min(),   df3.long.max(),
         df3.lat.min(), df3.lat.max()))

ruh_m = plt.imread('/Users/othman/Downloads/map.png')

fig, ax = plt.subplots(figsize = (8,7))
ax.scatter(df3.long, df3.lat, zorder=1, alpha= 0.2, c='b', s=10)
ax.set_title('Plotting Spatial Data on Map')
ax.set_xlim(BBox[0],BBox[1])
ax.set_ylim(BBox[2],BBox[3])
ax.imshow(ruh_m, zorder=0, extent = BBox, aspect= 'equal')

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt


df = pd.read_csv('/Users/othman/Downloads/senlatlon.csv')

geometry = [Point(xy) for xy in zip(df['lat'], df['long'])]
gdf = GeoDataFrame(df, geometry=geometry)

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
gdf.plot(ax=world.plot(figsize=(15, 15)), marker='o', color='red', markersize=15);
