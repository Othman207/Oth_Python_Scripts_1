from geopy.geocoders import Nominatim
from shapely.geometry import Point, LineString
import geopandas as gpd
import folium
import matplotlib.pyplot as plt
import descartes
import seaborn as sns
import numpy as np
import random
import csv
# = ['Accra', 'Cairo', 'Harare', 'Lagos', 'Dakar', 'Kumasi', 'Cape Town', 'Nairobi', 'Pretoria', 'Freetown', 'Algiers', 'Tripoli']

#city_list2 = ['Bagmati','Gandaki', 'Karnali', 'Lumbini', 'Madhesh', 'Sudurpaschim', 'Province 1']

# with open("locnepal.csv", "r") as f:
#         reader = csv.DictReader(f)
#         a = list(reader)
#         print (a)
#
#
locnepal = pd.read_csv('locnepal.tsv', index_col=False, header=None)
dict=locnepal.to_dict('list')
lis = list(dict.items())

city_list2 = ['Achham', 'Arghakhanchi','Baglung', 'Baitadi', 'Bajhang', 'Bajura', 'Banke', 'Bara', 'Bardiya', 'Bhaktapur', 'Bhojpur', 'Chitwan', 'Dadeldhura', 'Dailekh', 'Dang', 'Darchula', 'Dhading', 'Dhankuta', 'Dhanusa', 'Dhanusha', 'Dolakha', 'Dolpa', 'Doti', 'Gorkha', 'Gulmi', 'Humla', 'Illam', 'Jajarkot', 'Jhapa', 'Jumla', 'Kailali', 'Kalikot', 'Kanchanpur', 'Kapilvastu', 'Kaski', 'Kathmandu', 'Kavrepalanchok', 'Khotang', 'Lalitpur', 'Lamjung', 'Mahottari', 'Makwanpur', 'Manang', 'Morang', 'Mugu', 'Mustang', 'Myagdi', 'Nawalparasi', 'Nuwakot', 'Okhaldhunga', 'Palpa', 'Panchthar', 'Parbat', 'Parsa', 'Pyuthan', 'Ramechhap', 'Rasuwa', 'Rautahat', 'Rolpa', 'Rukum', 'Rupendehi', 'Salyan', 'Sankhuwasabha', 'Saptari', 'Sarlahi', 'Sindhuli', 'Sindhupalchowk', 'Siraha', 'Solukhumbu', 'Sunsari', 'Surkhet', 'Syangja', 'Tanahun', 'Taplejung', 'Tehrathum', 'Udayapur']
            #['Achham', 'Arghakhanchi', 'Baglung', 'Baitadi', 'Bajhang', 'Bajura', 'Banke', 'Bara', 'Bardiya', 'Bhaktapur', 'Bhojpur', 'Chitwan', 'Dadeldhura', 'Dailekh', 'Dang', 'Darchula', 'Dhading', 'Dhankuta', 'Dhanusa', 'Dhanusha', 'Dolakha', 'Dolpa', 'Doti', 'Gorkha', 'Gulmi', 'Humla', 'Illam', 'Jajarkot', 'Jhapa', 'Jumla', 'Kailali', 'Kalikot', 'Kanchanpur', 'Kapilvastu', 'Kaski', 'Kathmandu', 'Kavrepalanchok', 'Khotang', 'Lalitpur', 'Lamjung', 'Mahottari', 'Makwanpur', 'Manang', 'Morang', 'Mugu', 'Mustang', 'Myagdi', 'Nawalparasi', 'Nuwakot', 'Okhaldhunga', 'Palpa', 'Panchthar', 'Parbat', 'Parsa', 'Pyuthan', 'Ramechhap', 'Rasuwa', 'Rautahat', 'Rolpa', 'Rukum', 'Rupendehi', 'Salyan', 'Sankhuwasabha', 'Saptari', 'Sarlahi', 'Sindhuli', 'Sindhupalchowk', 'Siraha', 'Solukhumbu', 'Sunsari', 'Surkhet', 'Syangja', 'Tanahun', 'Taplejung', 'Tehrathum', 'Udayapur']

def get_coordinates(city_list2):
    """Takes a list of cities and returns a dictionary of the cities and their corresponding coordinates."""
    geolocator = Nominatim(user_agent="location script")
    dicto = {}

    for city in city_list2:
        try:
            location = geolocator.geocode(city)
            assert location, city
        except:
            raise Exception("There was a problem with the getCoordinates function")
        coordinate_values = (location.longitude,
                             location.latitude)  # in geopandas, the x value corresponds to the longitude while the y value, the latitude(Just in case you were wondering why it was *location.longitude, location.latitude* and not the other way round )
        dicto[city] = coordinate_values  # adding the coordinate pair to the dictionary at the end of every loop
    return dicto  # finally retruns the dict

city_coords_dict = get_coordinates(city_list2)
city_coords_dict

cities_geom = [Point(i) for i in city_coords_dict.values()]
d = {'Cities':[city for city in city_coords_dict.keys()], 'geometry': cities_geom} #we have to create a gdf so that we can add the crs information so we need geopandas now
cities_gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")
cities_gdf
cities_gdf.plot()

# load a sample geodataframe
nepal_gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
nepal_gdf = nepal_gdf[nepal_gdf['name'] == 'Nepal']

ax = nepal_gdf.plot(color='#e3bccf', edgecolor='blue')
plt.rcParams['figure.figsize'] = [10,
                                  10]  # we're able to call this because geopandas is built on top of pandas, which is built on top of  matplotlib

for x, y, label in zip(nepal_gdf.geometry.representative_point().x, nepal_gdf.geometry.representative_point().y,
                       nepal_gdf.name):
    ax.annotate(label, xy=(x, y))

cities_gdf.plot(ax=ax, color='orange')


#folium
gjson_cities = cities_gdf['geometry'].to_json()

my_map = folium.Map(tiles='cartodb positron')
cities = folium.features.GeoJson(gjson_cities)
my_map.add_child(cities)
my_map

#to view saved html
my_map.save('aee.html')


#mapping
street_map=gpd.read_file('nepalshpef.shp')
fig,ax = plt.subplots(figsize=(15,15))
street_map.plot(ax=ax)
plt.show()


#Calculating distance
import geopy.distance
coords_1 = (81.77144084, 28.0960951)
coords_2 = (83.03580398, 29.2144915)
geopy.distance.geodesic(coords_1, coords_2).km
#distance with dataframe
import h3
df = pd.read_csv('/Users/othman/Downloads/nds2.csv')
df['Dist'] = df.apply(lambda row: h3.point_dist((row['lat1'], row['long1']), (row['lat2'], row['long2']),unit='km'), axis=1)
df.to_csv("/Users/othman/Downloads/nepal_dist3.csv")

#calculating shortest distance


import pandas as pd
import geopy.distance
df = pd.read_csv('/Users/othman/Downloads/df1w.csv')
df2 = pd.read_csv('/Users/othman/Downloads/df2.csv')

for i,row in df.iterrows(): # A
    a = row.lat, row.long
    distances = []
    for j,row2 in df2.iterrows(): # B
        b = row2.lat, row2.long
        distances.append(geopy.distance.geodesic(a, b).km)

    min_distance = min(distances)
    min_index = distances.index(min_distance)
    print("A", i, "is closest to B", min_index, min_distance, "km")

    # for ind in df.index:
    # for indx in df2.index:
    #  print(df['District'][ind], i, "is closest to ",df2['Provincial Vaccine Store'][indx], min_index, min_distance, "km")

# df['Name'][ind], df['Stream'][ind]

# Mapping origin destination
import pandas as pd
import numpy as np
import folium

df = pd.read_csv('/Users/othman/Downloads/nds4.csv')

centroid_lat = 27.6953283
centroid_lon = 85.3035969

x = .1

n = 10

locations = df[['origin_lat', 'origin_lng']]
locationlist = locations.values.tolist()
len(locationlist)
locationlist[7]

f = folium.Figure(width=1000, height=500)
m = folium.Map([centroid_lat, centroid_lon], zoom_start=6, min_zoom = 6).add_to(f)


for point in range(0, len(locationlist)):
    folium.Marker(locationlist[point], popup=df['Province'][point]).add_to(m)


for _, row in df.iterrows():
    folium.Marker(locationlist[point], popup=df['Province'][point],
                  icon=folium.Icon(color='darkblue', icon_color='white', icon='building-o', angle=0, prefix='fa')).add_to(m)
    # folium.CircleMarker([row['origin_lat'], row['origin_lng']],
    #                     radius=15,
    #                     fill_color="#3db7e4", # divvy color
    #
    #                    ).add_to(m)

    folium.CircleMarker([row['destination_lat'], row['destination_lng']],
                        radius=15,
                        fill_color="red", # divvy color
                       ).add_to(m)

    folium.PolyLine([[row['origin_lat'], row['origin_lng']],
                     [row['destination_lat'], row['destination_lng']]]).add_to(m)
m

m.save('supmap.html')

# import json
# with open('data.json', 'w', encoding='utf-8') as f:
#     json.dump(cities, f, ensure_ascii=False, indent=4)


