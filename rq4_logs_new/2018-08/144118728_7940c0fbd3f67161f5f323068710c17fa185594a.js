import React, { Component } from 'react'
import { Text, View, TouchableOpacity, StyleSheet } from 'react-native'
import { green, red, blue } from '../utils/colors' 

class Quiz extends Component {
    render() {
        return(
            <View>
                <Text>2 / 2</Text>
                <View>
                    <Text>Here is the question?</Text>
                </View>
                <TouchableOpacity>
                    <Text>Answer</Text>
                </TouchableOpacity>
                <TouchableOpacity>
                    <Text>Correct</Text>
                </TouchableOpacity>
                <TouchableOpacity>
                    <Text>Incorrect</Text>
                </TouchableOpacity>
            </View>
        )
    }
}

export default Quiz

const styles = StyleSheet.create({
    button: {

    },
    buttonText: {
        
    }
})