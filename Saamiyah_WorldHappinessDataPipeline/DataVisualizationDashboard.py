import pandas as pd
import geopandas as gp
import holoviews as hv
hv.extension('bokeh')
import world_bank_data as wb
import folium

exec(open('DataPipeline.py').read())


######################################################################################################################################
#Part 5: Data Visualization Dashboard
######################################################################################################################################

#Read geographic details
geo_data  = gp.read_file('https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson')

#Read data modelling csv file to fetch happiness details
world_happiness = pd.read_csv("DataModelling.csv")
world_happiness_final = geo_data.merge(world_happiness, how="left", left_on=['ADMIN'], right_on=['Country'])

#Map plotting
x_map = world_happiness_final.centroid.x.mean()
y_map = world_happiness_final.centroid.y.mean()
 
world_map = folium.Map(location=[y_map, x_map], zoom_start=2,tiles=None)
folium.TileLayer('CartoDB positron',name='Dark Map',control=False).add_to(world_map)

#Create chloropet with color 
folium.Choropleth(
    geo_data=world_happiness_final,
    name='Choropleth',         
    data=world_happiness_final,
    columns=['Country', 'Happiness Score'],
    key_on='feature.properties.ADMIN',
    fill_color= 'RdYlGn',
    fill_opacity=0.6,
    line_opacity=0.8,
    legend_name='Happiness Status',
    smooth_factor=0,     
    highlight=True
).add_to(world_map)
 

#Style tooltip 
style_function = lambda x: {'fillColor': '#ffffff',
                            'color':'#000000',
                            'fillOpacity': 0.1,
                            'weight': 0.1}


#Create tooltip to show happiness details 
tooltip_data = folium.features.GeoJson(
    world_happiness_final,
    style_function=style_function,
    control=False,
    tooltip=folium.features.GeoJsonTooltip(
        fields=['Country'
                ,'Overall Rank'
                ,'Happiness Score'
               ],
        aliases=['Country: '
                ,'Happiness Rank: '
                ,'Happiness Score: '
                 ],
        style=('background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;')
    )
)

#Add tooltip layer to hover action
world_map.add_child(tooltip_data)
world_map.keep_in_front(tooltip_data)
folium.LayerControl().add_to(world_map)
 
world_map.save('GeoMap.html')




######################################################################################################################################
#Part 6: World Bank Data API
######################################################################################################################################


#fetch global data from World Bank Data API 
getAPIGlobalData = wb.get_countries()[['capitalCity', 'longitude', 'latitude', 'name']]
dataModellingCSV = pd.read_csv('DataModelling.csv')
mergedDataWithAPI = dataModellingCSV.merge(getAPIGlobalData, how="left", left_on=['Country'], right_on=['name'])

print(mergedDataWithAPI)



