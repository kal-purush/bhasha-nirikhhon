import React from 'react';
import { View, Text, StyleSheet, Image, TouchableOpacity } from 'react-native';
import MaterialCommunityIcons from 'react-native-vector-icons/MaterialCommunityIcons';
import * as Animatable from 'react-native-animatable';
import logo from '../../assets/lula.png';

export default function CardPeople(props) {
    return (
        <TouchableOpacity style={styles.container}>
            <View style={styles.containerLogo}>
                <Animatable.Image
                    animation="flipInY"
                    source={logo}
                    style={styles.image}
                    resizeMode="contain"
                />

                <View style={styles.text}>
                    <Text style={styles.textColor}>{props.name}</Text>
                </View>
            </View>


            <View style={styles.botoes}>
                <TouchableOpacity style={styles.confirmar}>
                    <Text style={styles.botoesText}>Confirmar</Text>
                </TouchableOpacity>
                <TouchableOpacity style={styles.excluir}>
                    <Text style={styles.botoesText}>Excluir</Text>
                </TouchableOpacity>
            </View>


        </TouchableOpacity>
    );
}

const styles = StyleSheet.create({
    container: {
        alignItems: 'flex-start',
        backgroundColor: 'white',
        flexDirection: 'row',
        height: 100,
        width: '100%',
        justifyContent: 'space-between',
        alignItems: 'center',
        paddingLeft: 20,
        paddingRight: 20
    },
    image: {
        width: 47,
        height: 45,
        borderRadius: 50
    },
    containerLogo: {
        backgroundColor: 'white',
        flexDirection: 'row',
        flex: 1.5
    },
    text: {
        justifyContent: 'space-between',
        marginLeft: 20
    },
    textColor: {
        color: '#2B0334'
    },
    botoes: {
        display: 'flex',
        flexDirection: 'row',
        width: '100%',
        alignItems: 'center',
        justifyContent: 'center',
        flex: 1
    },
    confirmar: {
        width: 70,
        backgroundColor: '#2B0334',
        height: 47,
        marginRight: 20,
        alignItems: 'center',
        justifyContent: 'center',
        borderRadius: 10
    },
    excluir: {
        width: 70,
        backgroundColor: '#2B0334',
        height: 47,
        alignItems: 'center',
        justifyContent: 'center',
        borderRadius: 10
    },
    botoesText: {
        color: 'white'
    }
})