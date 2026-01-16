class GoogleService {
    constructor() {
        this.styleDark = [
            {
                elementType: "geometry",
                stylers: [
                    {
                        color: "#eeeeee",
                    },
                ],
            },
            {
                elementType: "geometry.fill",
                stylers: [
                    {
                        visibility: "off",
                    },
                ],
            },
            {
                featureType: "landscape.natural.terrain",
                elementType: "geometry.fill",
                stylers: [
                    {
                        visibility: "on"
                    }
                ],
            },
            {
                featureType: "landscape.natural.landcover",
                elementType: "geometry.fill",
                stylers: [
                    {
                        visibility: "on"
                    }
                ],
            },
            {
                featureType: "landscape.natural",
                elementType: "geometry",
                stylers: [
                    {
                        color: "#dfd2ae",
                    },
                ],
            },
            {
                elementType: "labels.text.stroke",
                stylers: [
                    {
                        color: "#242f3e",
                    },
                ],
            },
            {
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#746855",
                    },
                ],
            },
            {
                featureType: "administrative.locality",
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#e11e11",
                    },
                ],
            },
            {
                featureType: "poi",
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#d59563",
                    },
                ],
            },
            {
                featureType: "poi.park",
                elementType: "geometry",
                stylers: [
                    {
                        color: "green",
                    },
                ],
            },
            {
                elementType: "labels.text.fill",
                featureType: "poi.park",
                stylers: [
                    {
                        color: "#6b9a76",
                    },
                ],
            },
            {
                featureType: "road",
                elementType: "geometry",
                stylers: [
                    {
                        color: "#38414e",
                    },
                ],
            },
            {
                featureType: "road",
                elementType: "geometry.stroke",
                stylers: [
                    {
                        color: "#212a37",
                    },
                ],
            },
            {
                featureType: "road",
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#9ca5b3",
                    },
                ],
            },
            {
                featureType: "road.highway",
                elementType: "geometry",
                stylers: [
                    {
                        color: "#746855",
                    },
                ],
            },
            {
                featureType: "road.highway",
                elementType: "geometry.stroke",
                stylers: [
                    {
                        color: "#1f2835",
                    },
                ],
            },
            {
                featureType: "road.highway",
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#f3d19c",
                    },
                ],
            },
            {
                featureType: "transit",
                elementType: "geometry",
                stylers: [
                    {
                        color: "#2f3948",
                    },
                ],
            },
            {
                featureType: "transit.station",
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#eeeeee",
                    },
                ],
            },
            {
                featureType: "water",
                elementType: "labels.text.fill",
                stylers: [
                    {
                        color: "#515c6d",
                    },
                ],
            },
            {
                featureType: "water",
                elementType: "labels.text.stroke",
                stylers: [
                    {
                        color: "#17263c",
                    },
                ],
            },
        ];

        this.styleTrans = [
            {
                elementType: "geometry.fill",
                stylers: [
                    {
                        visibility: "off",
                    },
                ],
            },
            {
                featureType: "landscape.natural.landcover",
                elementType: "geometry.fill",
                stylers: [
                    {
                        visibility: "on"
                    }
                ],
            },
        ];
    }

    getStyle() {
        return this.styleDark;
    }
}

export default new GoogleService();