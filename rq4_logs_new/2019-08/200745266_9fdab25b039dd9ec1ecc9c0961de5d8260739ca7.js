import React from 'react';
import { ScrollView, Image, StyleSheet } from 'react-native';



const Portfolio = ({uriArray}) => {
  return (
    <ScrollView contentContainerStyle={styles.contentContainer}>
      <Image
        style = {styles.portfolioPic}
        resizeMode = "contain"
        source = {{ uri: uriArray[0] }}
      />
      <Image
        style = {styles.portfolioPic}
        resizeMode = "contain"
        source = {{ uri: uriArray[1] }}
      />
      <Image
        style = {styles.portfolioPic}
        resizeMode = "contain"
        source = {{ uri: uriArray[2] }}
      />
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  portfolioPic: {
    flex:1,
    height: 250,
    width: "auto"
  },
  contentContainer: {
    paddingBottom: 50,
    paddingTop: 55
  },
})

export default Portfolio;