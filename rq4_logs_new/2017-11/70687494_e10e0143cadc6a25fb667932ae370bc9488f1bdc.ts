import { GoogleMapContainerProps } from "../components/GoogleMapContainer";
import { UrlHelper } from "./UrlHelper";

export interface Location {
    address?: string;
    latitude?: number;
    longitude?: number;
    url?: string;
}

export interface StaticLocation {
    address: string;
    latitude: string;
    longitude: string;
    icon: string;
}

export const getStaticMarkerUrl = (enumImage: string, defaultMakerIcon: string): string => {
    if (enumImage) {
        return UrlHelper.getStaticResourceUrl(enumImage);
    } else if (defaultMakerIcon) {
        return UrlHelper.getStaticResourceUrl(defaultMakerIcon);
    } else {
        return "";
    }
};

// Mendix does not support negative and decimal number as static inputs, so they are strings.
export const parseStaticLocations = (props: GoogleMapContainerProps): Location[] => {
    return props.staticLocations.map(location => ({
        address: location.address,
        latitude: location.latitude.trim() !== "" ? Number(location.latitude) : undefined,
        longitude: location.longitude.trim() !== "" ? Number(location.longitude) : undefined,
        url: getStaticMarkerUrl(location.icon, props.defaultMakerIcon)
    }));
};

export const parseStyle = (style = ""): {[key: string]: string} => { // Doesn't support a few stuff.
    try {
        return style.split(";").reduce<{[key: string]: string}>((styleObject, line) => {
            const pair = line.split(":");
            if (pair.length === 2) {
                const name = pair[0].trim().replace(/(-.)/g, match => match[1].toUpperCase());
                styleObject[name] = pair[1].trim();
            }
            return styleObject;
        }, {});
    } catch (error) {
        // tslint:disable-next-line no-console
        window.console.log("Failed to parse style", style, error);
    }

    return {};
};