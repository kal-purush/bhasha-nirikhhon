import type { DetailedHTMLProps, HTMLAttributes, ReactNode } from "react";
import h from "@macrostrat/hyper";

/**
 * @see https://react-leaflet.js.org/docs/example-react-control
 */

// Classes used by Leaflet to position controls.
const POSITION_CLASSES = {
  bottomleft: "leaflet-bottom leaflet-left",
  bottomright: "leaflet-bottom leaflet-right",
  topleft: "leaflet-top leaflet-left",
  topright: "leaflet-top leaflet-right",
} as const;

const MapCustomControl = (props: MapCustomControlProps): React.ReactNode => {
  const { position, containerProps, children } = props;
  return h("div", { className: POSITION_CLASSES[position] }, [
    h("div.leaflet-control.leaflet-bar", containerProps, children),
  ]);
};

export type MapCustomControlProps = {
  position: keyof typeof POSITION_CLASSES;
  containerProps?: DetailedHTMLProps<
    HTMLAttributes<HTMLDivElement>,
    HTMLDivElement
  >;
  children: ReactNode;
};

MapCustomControl.defaultProps = {
  position: "topleft" as MapCustomControlProps["position"],
  containerProps: {},
  children: null,
};

export default MapCustomControl;