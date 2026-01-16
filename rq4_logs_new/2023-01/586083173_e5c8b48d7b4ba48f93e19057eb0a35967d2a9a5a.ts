import { StyleSheet } from 'react-native';
import COLORS from '@constants/COLORS';

export const PeopleScreenStyle = StyleSheet.create({
    safeArea: {
        flex: 1
    },
    container: {
        paddingHorizontal: 5
    },
    title: {
        paddingTop: 10,
        fontSize: 26,
        color: COLORS.WHITE,
        alignSelf: 'center',
        fontWeight: 'bold'
    },
    inputView: {
        height: 40,
        marginTop: 15,
        borderWidth: 0,
        backgroundColor: COLORS.GRAY_100
    },
    input: {
        paddingLeft: 10,
        fontSize: 14,
        color: COLORS.GRAY_800,
        fontWeight: '500'
    },
    flashListView: {
        width: '100%',
        height: '100%',
        marginTop: 10
    },
    itemView: {
        height: 50,
        marginVertical: 5,
        paddingHorizontal: 12,
        borderRadius: 12,
        backgroundColor: COLORS.GRAY_100,
        flexDirection: 'row',
        justifyContent: 'space-between',
        alignItems: 'center'
    },
    itemTextName: {
        fontSize: 12.5,
        color: COLORS.WHITE,
        fontWeight: '500'
    },
    itemTextUsername: {
        fontSize: 12.5,
        color: COLORS.LIGHTGRAY,
        fontWeight: '500'
    },
    itemImage: {
        width: 32,
        height: 32,
        borderRadius: 25
    }
});