import { StyleSheet } from 'react-native';
import COLORS from '@constants/COLORS';

export const HangoutPickerStyle = StyleSheet.create({
    container: {
        padding: 15
    },
    title: {
        paddingLeft: 2,
        fontSize: 18,
        color: COLORS.WHITE,
        fontWeight: '600'
    },
    tagsRow: {
        paddingTop: 10,
        flexDirection: 'row'
    },
    tagsItem: {
        marginRight: 5,
        padding: 7,
        borderRadius: 15
    },
    peopleItem: {
        marginBottom: 5,
        marginRight: 5,
        padding: 7,
        borderRadius: 15,
        backgroundColor: COLORS.MAIN_BLUE
    },
    tagText: {
        fontSize: 15,
        color: COLORS.WHITE,
        fontWeight: '500'
    },
    inputContainer: {
        paddingTop: 20
    },
    input: {
        paddingLeft: 5,
        fontSize: 14,
        color: COLORS.WHITE,
        fontWeight: '500'
    },
    inputView: {
        marginTop: 10,
        height: 40,
        borderWidth: 0,
        backgroundColor: COLORS.GRAY_100
    }
});