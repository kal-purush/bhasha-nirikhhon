import { StyleSheet, Platform } from 'react-native';

export const patientInfoStyle = StyleSheet.create({
   
    container: {
        height: Platform.OS === 'android' ? '50%' : '40%',
        alignContent:'flex-end',
        alignSelf:'flex-end',
        width: Platform.OS === 'android' ? '100%' : '50%',
        backgroundColor: '#9dd4fb',
        paddingLeft: 20,
        paddingRight: 20,
       
    },
    text: { 
        backgroundColor: 'white',
        padding: 10,
    },
    button: { 
        justifyContent: 'center',
        backgroundColor: '#0274a1',
        width: Platform.OS === 'android' ? '50%' : '25%',
        alignSelf:'center',
        marginTop: 30,
        height: 40,
        borderRadius: 10,
    }
})