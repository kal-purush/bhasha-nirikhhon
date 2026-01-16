import { StyleSheet } from 'react-native';
import COLORS from '@constants/COLORS';

export const HeaderPlusStyle = StyleSheet.create({
    iconButton: {
        marginHorizontal: 12
    },
    modalContainer: {
        marginHorizontal: 15,
        padding: 15,
        borderRadius: 20,
        backgroundColor: COLORS.MAIN_BLUE,
        justifyContent: 'space-between'
    },
    title: {
        paddingLeft: 2,
        fontSize: 18,
        color: COLORS.WHITE,
        fontWeight: '600'
    },
    input: {
        paddingLeft: 10,
        fontSize: 14,
        color: COLORS.WHITE,
        fontWeight: '500'
    },
    inputView: {
        height: 40,
        marginTop: 15,
        borderWidth: 0,
        backgroundColor: COLORS.MAIN_BLUE_200
    },
    touchableOpacity: {
        marginTop: 20,
        alignSelf: 'center'
    },
    sendButton: {
        fontSize: 15,
        color: COLORS.WHITE,
        fontWeight: '600'
    }
});