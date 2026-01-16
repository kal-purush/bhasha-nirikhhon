import { Component, createElement } from "react";
import { Location, Map } from "./Map";
import { Alert } from "./Alert";

interface GoogleMapContainerProps {
    apiKey: string;
    contextObject: mendix.lib.MxObject;
    dataSource: DataSource;
    dataSourceMicroflow: string;
    defaultCenterAddress: string;
    entityConstraint: string;
    locationsEntity: string;
    addressAttribute: string;
    latitudeAttribute: string;
    longitudeAttribute: string;
    staticLocations: Location[];
}

type DataSource = "static" | "context" | "XPath" | "microflow";

class GoogleMapContainer extends Component<GoogleMapContainerProps, { alertMessage?: string, locations: Location[] }> {
    private subscriptionHandles: number[];
    constructor(props: GoogleMapContainerProps) {
        super(props);

        const alertMessage = this.validate();
        this.subscriptionHandles = [];
        this.state = { alertMessage, locations: [] };
        this.resetSubscriptions();
        this.createSubscriptions();
    }

    render() {
        if (this.state.alertMessage) {
            return createElement(Alert as any, { message: this.state.alertMessage });
        } else {
            return createElement(Map, {
                apiKey: this.props.apiKey,
                defaultCenterAddress: this.props.defaultCenterAddress,
                locations: this.state.locations
            });
        }
    }

    componentWillReceiveProps(nextProps: GoogleMapContainerProps) {
        this.resetSubscriptions();
        this.fetchData();
    }

    componentDidMount() {
        if (!this.state.alertMessage) this.fetchData();
    }

    componentWillUnmount() {
        this.resetSubscriptions();
    }

    private validate() {
        let message = "";
        if (this.props.dataSource === "static" && !this.props.staticLocations.length) {
            message = "At least one static location is required";
        }
        if (this.props.dataSource === "XPath" && !this.props.locationsEntity) {
            message = "The locations entity is required";
        }
        if (this.props.dataSource === "context" && !this.props.locationsEntity) {
            message = "The locations entity is required";
        }
        if (this.props.dataSource === "microflow" && !this.props.dataSourceMicroflow) {
            message = "A data source microflow is required";
        }

        return message;
    }

    private resetSubscriptions() {
        if (this.subscriptionHandles) {
            this.subscriptionHandles.map((handle: number) =>
                window.mx.data.unsubscribe(handle)
            );
        }
    }

    private createSubscriptions() {
        if (this.props.contextObject) {
            const contextObject = this.props.contextObject;
            this.subscriptionHandles.push(window.mx.data.subscribe({
                callback: () => this.fetchData(),
                guid: this.props.contextObject.getGuid()
            }));
            const attributeList = [
                this.props.addressAttribute,
                this.props.latitudeAttribute,
                this.props.longitudeAttribute
            ];
            attributeList.forEach((attr) =>
                this.subscriptionHandles.push(window.mx.data.subscribe({
                    attr,
                    callback: () => this.fetchData(),
                    guid: contextObject.getGuid()
                }))
            );
        }
    }

    private fetchData() {
        if (this.props.dataSource === "static") {
            this.setState({ locations: this.props.staticLocations });
        } else if (this.props.dataSource === "context" && this.props.locationsEntity) {
            this.fetchLocationsByContext(this.props.contextObject);
        } else if (this.props.dataSource === "XPath" && this.props.locationsEntity) {
            this.fetchLocationsByXPath(this.props.contextObject ? this.props.contextObject.getGuid() : "");
        } else if (this.props.dataSource === "microflow" && this.props.dataSourceMicroflow) {
            this.fetchLocationsByMicroflow(this.props.dataSourceMicroflow);
        }
    }

    private fetchLocationsByContext(contextObject?: mendix.lib.MxObject) {
        if (contextObject) {
            this.setLocationsFromMxObjects([ contextObject ]);
        }
    }

    private fetchLocationsByXPath(contextGuid: string) {
        const { entityConstraint } = this.props;
        const requiresContext = entityConstraint && entityConstraint.indexOf("[%CurrentObject%]") > -1;
        if (!contextGuid && requiresContext) {
            this.setState({ locations: [] });
            return;
        }

        const constraint = entityConstraint ? entityConstraint.replace("[%CurrentObject%]", contextGuid) : "";
        window.mx.data.get({
            callback: mxObjects => this.setLocationsFromMxObjects(mxObjects),
            error: error =>
                this.setState({
                    alertMessage: `An error occurred while retrieving items: ${error}`,
                    locations: []
                }),
            xpath: `//${this.props.locationsEntity}${constraint}`
        });
    }

    private fetchLocationsByMicroflow(microflow: string) {
        if (microflow) {
            window.mx.ui.action(microflow, {
                callback: (mxObjects: mendix.lib.MxObject[]) => this.setLocationsFromMxObjects(mxObjects),
                error: error =>
                    this.setState({
                        alertMessage: `An error occurred while retrieving locations: ${error.message}`,
                        locations: []
                    }),
                params: {
                    guids: this.props.contextObject ? [ this.props.contextObject.getGuid() ] : []
                }
            });
        }
    }

    private setLocationsFromMxObjects(mxObjects: mendix.lib.MxObject[]): void {
        const locations = mxObjects.map((mxObject) => ({
            address: mxObject ? mxObject.get(this.props.addressAttribute) as string : undefined,
            latitude: mxObject ? Number(mxObject.get(this.props.latitudeAttribute)) : undefined,
            longitude: mxObject ? Number(mxObject.get(this.props.longitudeAttribute)) : undefined
        }));

        this.setState({ locations });
    }
}

export { GoogleMapContainer as default, GoogleMapContainerProps, DataSource };