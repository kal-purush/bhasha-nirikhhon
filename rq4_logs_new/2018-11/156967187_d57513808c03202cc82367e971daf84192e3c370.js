import React, {Component} from 'react';
import {StyleSheet, Text, View} from 'react-native';
import {LinearGradient} from 'expo';
import {Ionicons} from "@expo/vector-icons";

export default class Weather extends Component {
  render() {
    return <LinearGradient
      colors={["#00C6FB", "#005BEA", "black"]}
      style={styles.container}
    >
      <View style={styles.upper}>
        <Ionicons color="gray" size={144} name={"ios-rainy"}/>
        <Text style={styles.temp}>20˚</Text>
      </View>
      <View style={styles.lower}>
        <Text style={styles.title}>Raining like MF</Text>
        <Text style={styles.subTitle}>For more info look outside</Text>
      </View>
    </LinearGradient>;
  }
};

const styles = StyleSheet.create({
  container: {
    flex: 1
  },
  temp: {
    fontSize: 24,
    backgroundColor: "transparent",
    color: "white",
    marginBottom: 24
  },
  upper: {
    flex: 1,
    alignItems: "center",
    backgroundColor: "transparent",
    justifyContent: "center"
  },
  lower: {
    fontSize: 48,
    flex: 1,
    alignItems: "flex-start",
    justifyContent: "flex-end",
    paddingLeft: 25
  },
  title: {
    fontSize: 38,
    backgroundColor: "transparent",
    color: "white"
  },
  subTitle: {
    fontSize: 24,
    backgroundColor: "transparent",
    color: "white",
    marginBottom: 24
  }
})
