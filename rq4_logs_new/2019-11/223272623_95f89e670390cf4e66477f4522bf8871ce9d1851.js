import React from 'react';
import { View, Text, StyleSheet, Image } from 'react-native';

const ImageDetail = ({image, title, id}) => {
   return (
      <View>
         <Image source={image} style={{width: 200, height: 200}} />
         <Text>
            {title}
         </Text>
         <Text>
            Image id -> {id}
         </Text>
      </View>
   );
};

const styles = StyleSheet.create({});

export default ImageDetail;