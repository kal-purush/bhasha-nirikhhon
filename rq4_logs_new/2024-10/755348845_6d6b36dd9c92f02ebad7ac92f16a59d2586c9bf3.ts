import { FillLayer, LineLayer, BackgroundLayer } from "react-map-gl/maplibre"

const style = {
  boothFillColor: "#89bc82",
  boothOutlineColor: "#0e3e08",

  boothActiveFillColor: "#21c00d",
  boothHoveredFillColor: "#a0df98",
  boothNotFilteredOpacity: 0.4,

  buildingOutlineColor: "#ff0000",
  buildingOutlineWidth: 2,

  backgroundColor: "#40d07e",
  backgroundOpacity: 0.2
} as const

// features can be styled based on properties or feature state using a weird expression language, see:
// https://docs.mapbox.com/style-spec/reference/expressions/#data-expressions for reference

export const boothLayerStyle: FillLayer = {
  source: "booths",
  id: "booths",
  type: "fill",
  paint: {
    "fill-opacity": [
      "case",
      ["boolean", ["feature-state", "filtered"], false],
      1,
      style.boothNotFilteredOpacity
    ],
    "fill-outline-color": style.boothOutlineColor,
    "fill-color": [
      "case",
      ["boolean", ["feature-state", "active"], false],
      style.boothActiveFillColor,
      ["boolean", ["feature-state", "hover"], false],
      style.boothHoveredFillColor,
      style.boothFillColor
    ]
  }
}

export const buildingLayerStyle: LineLayer = {
  source: "buildings",
  id: "buildings",
  type: "line",
  paint: {
    "line-color": style.buildingOutlineColor,
    "line-width": style.buildingOutlineWidth
  }
}

export const backgroundLayerStyle: BackgroundLayer = {
  id: "background",
  type: "background",
  paint: {
    "background-color": style.backgroundColor,
    "background-opacity": style.backgroundOpacity
  }
}