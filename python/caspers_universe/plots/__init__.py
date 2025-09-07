import folium
import h3

from caspers_universe._internal import Site


def plot_site(
    site: Site, tiles="CartoDB dark_matter", popup="Chef Casper's Kitchen"
) -> folium.Map:
    m = folium.Map(
        location=[site.latitude, site.longitude],
        zoom_start=13,
        tiles=tiles,
    )

    cell = h3.latlng_to_cell(site.latitude, site.longitude, 9)
    location = h3.cells_to_geo([cell])
    folium.GeoJson(location).add_to(m)

    cells = h3.grid_disk(cell, 10)
    delivery_area = h3.cells_to_geo(cells)
    folium.GeoJson(delivery_area).add_to(m)

    folium.Marker(
        location=[site.latitude, site.longitude],
        popup=popup,
        icon=folium.Icon(color="red", icon="kitchen-set", prefix="fa"),
    ).add_to(m)

    return m
