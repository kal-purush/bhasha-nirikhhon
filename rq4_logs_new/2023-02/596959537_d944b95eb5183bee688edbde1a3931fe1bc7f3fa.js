import React from 'react'
import { Image, Text, ScrollView, StyleSheet } from 'react-native'

import { item } from './DATA'
import EnableFeatureList from './EnableFeatureList'

const HeadspaceNotification = () => (
<ScrollView style={styles.headspace_notification} showsVerticalScrollIndicator={false}>
<Image
    style={styles.background_image}
    source={{uri: item.background_image}}
    />
<Text style={styles.time_title}>{item.time_title}</Text>
<Text style={styles.about_time}>{item.about_time}</Text>
<EnableFeatureList item={item.enable_feature_list}/>
</ScrollView>
)

export default HeadspaceNotification;

const styles = StyleSheet.create({
background_image: {
    width: '100vw',
    height: '100vw',
    marginTop: 5
  },
time_title: {
    fontSize: 12,
    fontWeight: '250',
    paddingHorizontal: 10,
    paddingTop:5
  },
about_time: {
    fontSize: 12,
    fontWeight: '250',
    paddingHorizontal: 10,
    paddingTop:5
  }
});