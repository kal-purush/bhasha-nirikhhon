import { LocationObject } from "./components/Map";

interface MapDataOptions {
    apiKey: string;
    contextObject?: mendix.lib.MxObject;
    dataSource: DataSource;
    dataSourceMicroflow: string;
    entityConstraint: string;
    locationsEntity: string;
    locationAddressAttribute: string;
    locationLatitudeAttribute: string;
    locationLongitudeAttribute: string;
    staticLocations: LocationObject[];
}

type DataSource = "static" | "XPath" | "microflow";

class MapData {
    private options: MapDataOptions;
    private callback: (alert?: string, locations?: LocationObject[]) => void;

    constructor(options: MapDataOptions, callback: (alert?: string, locations?: LocationObject[]) => void) {
        this.callback = callback;
        this.options = options;
    }

    validateAndFetch() {
        let alert = "";
        if (this.options.dataSource === "static" && !this.options.staticLocations.length) {
            alert = "At least one static location is required";
        }
        if (this.options.dataSource === "XPath" && !this.options.locationsEntity) {
            alert = "The locations entity is required";
        }
        if (this.options.dataSource === "microflow" && !this.options.dataSourceMicroflow) {
            alert = "A data source microflow is required";
        }

        if (alert) {
            this.callback(alert);
        } else {
            this.fetchData();
        }
    }

    setContext(contextObject: mendix.lib.MxObject): MapData {
        this.options.contextObject = contextObject;

        return this;
    }

    private fetchData() {
        if (this.options.dataSource === "static") {
            this.callback(undefined, this.options.staticLocations);
        }
        if (this.options.dataSource === "XPath" && this.options.locationsEntity) {
            this.fetchLocationsByXPath(this.options.contextObject ? this.options.contextObject.getGuid() : "");
        } else if (this.options.dataSource === "microflow" && this.options.dataSourceMicroflow) {
            this.fetchLocationsByMicroflow(this.options.dataSourceMicroflow);
        }
    }

    private fetchLocationsByXPath(contextGuid: string) {
        const { entityConstraint } = this.options;
        const requiresContext = entityConstraint && entityConstraint.indexOf("[%CurrentObject%]") > -1;
        if (!contextGuid && requiresContext) {
            this.callback(undefined, []);
            return;
        }

        const constraint = entityConstraint ? entityConstraint.replace("[%CurrentObject%]", contextGuid) : "";
        window.mx.data.get({
            callback: (mxObjects: mendix.lib.MxObject[]) => this.setLocationsFromMxObjects(mxObjects),
            error: (error) =>
                this.callback(`An error occurred while retrieving items: ${error}`),
            microflow:"",
            path: `//${this.options.locationsEntity}${constraint}`
        });
    }

    private fetchLocationsByMicroflow(microflow: string) {
        if (microflow) {
            window.mx.ui.action(microflow, {
                callback: (mxObjects: mendix.lib.MxObject[]) => this.setLocationsFromMxObjects(mxObjects),
                error: (error: Error) =>
                    this.callback(`An error occurred while retrieving locations: ${error.message}`),
                params: {
                    guids: this.options.contextObject ? [ this.options.contextObject.getGuid() ] : []
                }
            });
        }
    }

    private setLocationsFromMxObjects(mxObjects: mendix.lib.MxObject[]): void {
        const locations = mxObjects.map((mxObject) => ({
            address: mxObject ? mxObject.get(this.options.locationAddressAttribute) as string : undefined,
            latitude: mxObject ? mxObject.get(this.options.locationLatitudeAttribute) as string : undefined,
            longitude: mxObject ? mxObject.get(this.options.locationLongitudeAttribute) as string : undefined
        }));

        this.callback(undefined, locations);
    }
}

export { MapData, MapDataOptions, DataSource };