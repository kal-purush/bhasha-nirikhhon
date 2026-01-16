import React from 'react'
import {View, Text, StyleSheet} from 'react-native'
const ScreenOne = () => {
    return(
        <View style={styles.container}>
        <Text>This is the first screeen</Text>
        </View>
    )
}

const ScreenTwo = () =>{
    return(
         <View style={styles.container}>
        <Text>This is the second screeen</Text>
        </View>
    )
}

const ScreenThree = () =>{
    return(
         <View style={styles.container}>
        <Text>This is the third screeen</Text>
        </View>
    )
}

const styles = StyleSheet.create({
    container:{
        flex: 1,
        backgroundColor: '#fff',
        justifyContent: 'center',
        alignItems: 'center'
    }
})
export {ScreenOne, ScreenTwo, ScreenThree}