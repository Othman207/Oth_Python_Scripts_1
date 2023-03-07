from openrouteservice.client import Client
#from .models import Data, Search, Distance
import folium
from folium import plugins
from folium.plugins import MarkerCluster, FastMarkerCluster
import geocoder
#from .forms import SearchForm, DistanceForm
from geopy.distance import geodesic
import openrouteservice as ors

ors_key = ''
client = ors.Client(key=ors_key)
coordinates = [[-86.781247, 36.163532], [-80.191850, 25.771645]]
#coordinates = [[s_lon, s_lat], [d_lon, d_lat]]

# directions
route = client.directions(coordinates=coordinates,
                          profile='driving-car',
                          format='geojson')

route_time = route['features'][0]['properties']['segments'][0]['distance'] * 0.0000277778

qs = []

for index, i in enumerate(route['features'][0]['properties']['segments'][0]['steps']):
    qs.append(i)

map1 = folium.Map(location=[19, -12],
                  zoom_start=2, control_scale=True, prefer_canvas=True)
map1.save('rmap.html')
plugins.HeatMap(data_list).add_to(map1)
plugins.Fullscreen(position='topright').add_to(map1)
