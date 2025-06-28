import folium
from folium.plugins import TimestampedGeoJson
import random
from datetime import datetime, timedelta


def generate_sample_trip(start_lat, start_lon, num_points=10):
    """Generate a sample trip with random movement."""
    points = []
    line_coordinates = []
    current_lat, current_lon = start_lat, start_lon

    # Generate timestamps starting from now
    base_time = datetime.now()

    for i in range(num_points):
        # Add some random movement
        current_lat += random.uniform(-0.001, 0.001)
        current_lon += random.uniform(-0.001, 0.001)

        # Store coordinates for the line
        line_coordinates.append([current_lon, current_lat])

        # Create timestamp for this point
        timestamp = base_time + timedelta(minutes=i * 5)

        # Add the point
        points.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [current_lon, current_lat],
                },
                "properties": {
                    "time": timestamp.isoformat(),
                    "popup": f"Point {i + 1}",
                    "icon": "circle",
                    "iconstyle": {
                        "fillColor": "red",
                        "fillOpacity": 0.6,
                        "stroke": "false",
                        "radius": 5,
                    },
                },
            }
        )

        # Add a line segment up to this point
        if i > 0:  # Start adding line segments after the first point
            points.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": line_coordinates[
                            : i + 1
                        ],  # Only include coordinates up to current point
                    },
                    "properties": {
                        "time": timestamp.isoformat(),  # Line segment appears with its endpoint
                        "style": {"color": "blue", "weight": 3, "opacity": 0.6},
                    },
                }
            )

    return points


def plot_trip():
    # Create a map centered at the starting point
    start_lat, start_lon = 37.7749, -122.4194  # San Francisco coordinates
    m = folium.Map(location=[start_lat, start_lon], zoom_start=15)

    # Generate sample trip data
    features = generate_sample_trip(start_lat, start_lon)

    # Create the TimestampedGeoJson layer
    TimestampedGeoJson(
        {"type": "FeatureCollection", "features": features},
        period="PT5M",  # 5 minutes between points
        duration="PT1M",  # 1 minute display duration for each point
        auto_play=True,
        loop=True,
        max_speed=1,
        loop_button=True,
        date_options="YYYY/MM/DD HH:mm:ss",
        time_slider_drag_update=True,
    ).add_to(m)

    # Save the map
    m.save("trip_visualization.html")
    print("Map saved as 'trip_visualization.html'")


if __name__ == "__main__":
    plot_trip()
