import React from 'react';
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native';

import ApprovedComponent from '../components/ApprovedComponent';

const FinishScreen = props => {
    
    return (
        <View style={styles.screen}>

            <ApprovedComponent/>
            
            <View style={styles.bottomButtons}>
                <View style={styles.textButtonContainer}>
                    <TouchableOpacity
                        activeOpacity={0.4} 
                        onPress={() => {
                            props.navigation.popToTop();
                        }}>
                        <View>
                            <Text style={styles.textButton}>FINISH</Text>
                        </View>
                    </TouchableOpacity>
                </View>      
            </View>
            
        </View>
    )
}

const styles = StyleSheet.create({
    screen: {
        flex: 1,
        justifyContent: 'space-around',
        alignItems: 'center',
        marginTop: 70,
        marginBottom: 70
    },
    bottomButtons: {
        flex: 1,
        flexDirection: "row",
        justifyContent: 'space-between',
        alignItems: 'center',
        maxHeight: '13%'
    },
    textButtonContainer: {
        minWidth: '23%',
        marginHorizontal: 10
    },
    textButton: {
        color: 'white',
        backgroundColor: "#002c4f",
        borderWidth: 1,
        borderRadius: 6,
        borderColor: "#002c4f",
        minHeight: '70%',
        textAlign: 'center',
        textAlignVertical: 'center'        
    }
});

export default FinishScreen