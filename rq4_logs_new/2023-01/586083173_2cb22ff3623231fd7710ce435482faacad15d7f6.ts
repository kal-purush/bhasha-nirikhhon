import { StyleSheet } from 'react-native';
import COLORS from '@constants/COLORS';

export const EventScreenStyle = StyleSheet.create({
    contentContainer: {
        flex: 1,
        paddingTop: 15,
        paddingBottom: 125,
        alignItems: 'center',
        justifyContent: 'space-between'
    },
    picturesView: {
        marginTop: 15,
        flexWrap: 'wrap',
        flexDirection: 'row',
        justifyContent: 'center'
    },
    imageView: {
        margin: 10
    },
    image: {
        width: 115,
        height: 115,
        borderRadius: 65
    },
    nameText: {
        marginTop: 15,
        fontSize: 15,
        color: COLORS.WHITE,
        alignSelf: 'center',
        fontWeight: '500'
    },
    timeText: {
        fontSize: 25
    },
    buttonsRow: {
        justifyContent: 'space-evenly',
        flexDirection: 'row'
    },
    row: {
        height: 40,
        flex: 1,
        marginHorizontal: 10,
        borderRadius: 10,
        backgroundColor: COLORS.MAIN_BLUE,
        justifyContent: 'center',
        alignItems: 'center'
    },
    buttonText: {
        fontSize: 15,
        color: COLORS.WHITE,
        fontWeight: '500'
    }
});