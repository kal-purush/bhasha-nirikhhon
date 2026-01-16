
import maplibregl from "maplibre-gl";
import "mapbox-gl/dist/mapbox-gl.css";

const generateMap = (container, hikingRoute) => {
    const { lon, lat } = hikingRoute
    const center = [lon, lat];
    const map = new maplibregl.Map({
        container,
        style:
        "https://api.maptiler.com/maps/streets/style.json?key=get_your_own_OpIi9ZULNHzrESv6T2vL",
        center,
        zoom: 10,
    });
    new maplibregl.Marker().setLngLat(center).addTo(map);
    return map
}
export default generateMap