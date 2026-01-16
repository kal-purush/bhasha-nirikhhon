import { StyleSheet } from 'react-native';
import COLORS from '@constants/COLORS';
import SHADOW from '@constants/SHADOW';

export const ChatItemStyle = StyleSheet.create({
    item: {
        marginBottom: 5,
        padding: 7.5,
        paddingHorizontal: 10,
        borderRadius: 20,
        alignSelf: 'flex-start'
    },
    alignFlexEnd: {
        alignSelf: 'flex-end'
    },
    lightBorder: {
        padding: 6.5,
        borderWidth: 1,
        borderColor: COLORS.WHITE
    },
    darkBorder: {
        ...SHADOW.CHAT_ITEM
    },
    text: {
        margin: 4,
        color: COLORS.WHITE,
        fontWeight: '600'
    }
});