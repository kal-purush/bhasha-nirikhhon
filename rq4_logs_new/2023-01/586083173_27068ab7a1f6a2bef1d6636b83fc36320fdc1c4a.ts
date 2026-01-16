import { StyleSheet } from 'react-native';
import COLORS from '@constants/COLORS';

export const LoginScreenStyle = StyleSheet.create({
    inputsContainer: {
        marginTop: '60%',
        paddingHorizontal: 40
    },
    inputView: {
        marginTop: 15
    },
    loginText: {
        marginTop: '20%',
        color: COLORS.WHITE,
        alignSelf: 'center',
        fontSize: 20,
        fontWeight: 'bold'
    },
    registerText: {
        marginTop: 100,
        color: COLORS.WHITE,
        alignSelf: 'center',
        fontSize: 18,
        fontWeight: 'bold'
    }
});