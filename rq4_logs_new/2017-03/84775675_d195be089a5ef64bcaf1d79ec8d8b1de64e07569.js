import React from 'react';
import { View, Text } from 'react-native';
import { AnimatedCircularProgress } from 'react-native-circular-progress';

const Gauge = ({ value }) => {
    return (
        <View>
            <AnimatedCircularProgress
              size={120}
              width={15}
              fill={value}
              tintColor="#00e0ff"
              backgroundColor="#3d5875"
            >
            {
                () => <Text>{value} KM/h!!!</Text>
            }
            </AnimatedCircularProgress>
        </View>
    );
};

export { Gauge };