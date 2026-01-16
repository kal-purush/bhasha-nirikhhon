import React, { Component } from 'react';
import {
    Dimensions,
    View,
    Text,
    ScrollView,
    PermissionsAndroid,
    Platform,
    AsyncStorage,
    StyleSheet,
    TextInput,
    TouchableOpacity,
} from 'react-native';
import EStyleSheet from 'react-native-extended-stylesheet';
import MapView from 'react-native-maps';
import { KEY } from '../../constants/misc';
import Geolocation from '@react-native-community/geolocation';
const entireScreenWidth = Dimensions.get('window').width;
EStyleSheet.build({ $rem: entireScreenWidth / 380 });

class MapScreen extends Component {
    constructor(props) {
        super(props);
        this.state = {
            error: "",
            latitude: 0,//Initial Longitude
            longitude: 0,//Initial Latitude
            destination: "",
            predictions: []
        };

    }

    componentDidMount() {
        Geolocation.getCurrentPosition(
            position => {
                this.setState({
                    latitude: position.coords.latitude,
                    longitude: position.coords.longitude
                });
            },
            error => this.setState({ error: error.message }),
            { enableHighAccuracy: true, maximumAge: 2000, timeout: 2000 }
        );

    }

    async onChangeDestination(destination) {
        //console.log(destination);
        this.setState({ destination });
        const apiUrl = `https://maps.googleapis.com/maps/api/place/autocomplete/json?key=${KEY}&components=country:lk&types=(cities)&&input=${destination}&location=${this.state.latitude},${this.state.longitude}`;
        try {
            const result = await fetch(apiUrl);
            const json = await result.json();
            //console.log(json);
            this.setState({
                predictions: json.predictions
            });
        } catch (e) {
            console.log(e);
        }
    }

    async handleSelectedAddress(placeID) {
        const apiUrl = `https://maps.googleapis.com/maps/api/place/details/json?input=bar&placeid=${placeID}&key=${KEY}`;
        try {
            const result = await fetch(apiUrl);
            const json = await result.json();
            console.log(json.result.formatted_address);
            this.setState({
                latitude: json.result.geometry.location.lat,
                longitude: json.result.geometry.location.lng,
                destination: json.result.formatted_address,
                predictions: []
            });

        } catch (e) {
            console.log(e);
        }
    }

    render() {
        const predictions = this.state.predictions.map(prediction => (
            <TouchableOpacity
                key={prediction.id}
                onPress={() => this.handleSelectedAddress(prediction.place_id)}
            >
                <Text style={styles.suggestions} >{prediction.description}</Text>
            </TouchableOpacity>
        ));
        return (
            <View style={styles.container}>
                <MapView
                    style={styles.map}
                    region={{
                        latitude: this.state.latitude,
                        longitude: this.state.longitude,
                        latitudeDelta: 0.015,
                        longitudeDelta: 0.0121,
                    }}
                    showsUserLocation={true}
                >
                    <MapView.Marker
                        coordinate={{ latitude: this.state.latitude, longitude: this.state.longitude }}

                    />
                </MapView>
                <TextInput
                    placeholder="Enter place..."
                    style={styles.descriptionInput}
                    value={this.state.destination}
                    onChangeText={destination => this.onChangeDestination(destination)}
                />
                {predictions}
            </View>

        );
    }
}

const styles = StyleSheet.create({
    container: {
        ...StyleSheet.absoluteFillObject
    },
    map: {
        ...StyleSheet.absoluteFillObject,
    },
    descriptionInput: {
        height: 40,
        borderWidth: 0.5,
        marginTop: 50,
        marginLeft: 5,
        marginRight: 5,
        padding: 5,
        backgroundColor: "white",

    },
    suggestions: {
        backgroundColor: "white",
        padding: 5,
        fontSize: 18,
        borderWidth: 0.5,
        marginLeft: 5,
        marginRight: 5
    }

});

export default MapScreen;