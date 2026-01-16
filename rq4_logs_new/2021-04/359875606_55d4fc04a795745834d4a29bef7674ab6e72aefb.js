import React from "react";
import styles from "../styles.js";
import MapView, { PROVIDER_GOOGLE, Marker } from "react-native-maps";
import Entypo from "react-native-vector-icons/Entypo";
import MaterialCommunityIcons from "react-native-vector-icons/MaterialCommunityIcons";
import { SafeAreaView, Text, View } from "react-native";

export default function MapScreen() {
  const lights = lightData.map((marker, index) => (
    <Marker
      key={index}
      coordinate={marker.latLng}
      title={marker.title}
      description={marker.description}
    >
      <Entypo name="warning" size={40} color="#fada39" />
    </Marker>
  ));

  const crimes = crimeData.map((marker, index) => (
    <Marker
      key={index}
      coordinate={marker.latLng}
      title={marker.title}
      description={marker.description}
    >
      <MaterialCommunityIcons name="handcuffs" size={40} />
    </Marker>
  ));

  const usersLoc = (
    <Marker coordinate={userGeoData.latLng} title="Current Location" />
  );
  return (
    <SafeAreaView style={styles.container}>
      <View>
        <MapView
          style={styles.map}
          provider={PROVIDER_GOOGLE}
          initialRegion={regionConfig}
        >
          {lights}
          {crimes}
          {usersLoc}
        </MapView>
      </View>
      <Text>Our Map Screen</Text>
    </SafeAreaView>
  );
}

const regionConfig = {
  latitude: 34.0689,
  longitude: -118.4452,
  latitudeDelta: 0.02,
  longitudeDelta: 0.02,
};

const lightData = [
  {
    latLng: {
      latitude: 34.067,
      longitude: -118.45,
    },
    title: "Unsafe Intersection",
    description: "No Light Fixings!",
  },
  {
    latLng: {
      latitude: 34.067,
      longitude: -118.447,
    },
    title: "Unsafe Intersection",
    description: "No Light Fixings!",
  },
];

const crimeData = [
  {
    latLng: {
      latitude: 34.067,
      longitude: -118.43,
    },
    title: "Unsafe Intersection",
    description: "High Crime Rating in Past 24 hrs!",
  },
  {
    latLng: {
      latitude: 34.067,
      longitude: -118.47,
    },
    title: "Unsafe Intersection",
    description: "Crime History!",
  },
];

const userGeoData = {
  latLng: {
    latitude: 34.0689,
    longitude: -118.4452,
  },
};