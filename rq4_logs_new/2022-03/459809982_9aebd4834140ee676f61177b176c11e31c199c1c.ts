import { StyleSheet } from 'react-native';

import { color, radius, scaleSize, spacing } from '../../styles';

export default StyleSheet.create({
    default: {
        flexDirection: 'row',
        flexWrap: 'wrap',
        alignItems: 'center',
        marginBottom: spacing('extraLoose'),
        paddingBottom: spacing('extraLoose'),
    },
    body: {
        flexBasis: 1,
        flexGrow: 1,
        flexShrink: 0,
        marginRight: spacing('extraLoose'),
    },
    bordered: {
        borderBottomWidth: 1,
        borderBottomColor: color('black', '70'),
    },
    lastChild: {
        borderBottomWidth: 0,
        marginBottom: 0,
        paddingBottom: 0,
    },
    button: {
        width: scaleSize(40),
        height: scaleSize(62),
        borderRadius: radius('smaller'),
        alignItems: 'center',
        justifyContent: 'center',
        backgroundColor: color('black', '90'),
    },
});