interface Coordinate {
  lat: number;
  lng: number;
}
interface Property {
  rajoitus_maksullinen_arkena: string;
  rajoitus_maksullinen_lauantaina: string;
  rajoitus_maksullinen_sunnuntaina: string;
}
interface PolygonType {
  id: string;
  properties: Property;
}