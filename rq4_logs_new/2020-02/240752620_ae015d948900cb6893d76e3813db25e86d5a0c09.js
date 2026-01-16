import React from 'react';
import {View, Text, StyleSheet, Image} from 'react-native';

const WeatherInformation = ({weatherInfo}) => {
  return (
    <View style={styles.container}>
      <View>
        {/* this should be in i18n */}
        <Text>Temperatura mínima: </Text>
        <Text>{weatherInfo.temp_min}</Text>
        <Text>Temperatura máxima: </Text>
        <Text>{weatherInfo.temp_max}</Text>
        <Text>Temperatura actual: </Text>
        <Text>{weatherInfo.temp}</Text>
        <Text>Presión: </Text>
        <Text>{weatherInfo.pressure}</Text>
        <Text>Humedad: </Text>
        <Text>{weatherInfo.humidity}</Text>
      </View>
      <View>
        {weatherInfo.temp < 50 ? (
          <Image
            source={require('./images/rain-icon.png')}
            style={{height: 200, width: 200}}
          />
        ) : (
          <Image
            source={require('./images/rain-icon.png')}
            style={{height: 200, width: 200}}
          />
        )}
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    flexDirection: 'row',
    justifyContent: 'space-evenly',
  },
});

export default WeatherInformation;