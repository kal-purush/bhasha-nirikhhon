import { StyleSheet } from 'react-native';
import { radius, color, spacing, scaleSize } from '../../styles';

export default StyleSheet.create({
    container: {
        position: 'absolute',
        bottom: spacing(),
        left: 0,
        right: 0,
        marginHorizontal: spacing('extraLoose'),
        borderRadius: radius('large'),
        padding: spacing(),
        flexDirection: 'row',
        justifyContent: 'space-around',
        backgroundColor: color('black'),
        zIndex: 1,
    },
    button: {
        width: scaleSize(52),
        height: scaleSize(52),
        justifyContent: 'center',
        alignItems: 'center',
        borderRadius: radius(),
    },
});