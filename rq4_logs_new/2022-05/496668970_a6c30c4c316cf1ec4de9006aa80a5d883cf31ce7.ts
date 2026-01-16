import {StyleSheet} from 'react-native'
import { theme } from '../../global/styles/theme'

export const styles = StyleSheet.create({
    container:{
        width: 104,
        height: 120,
        justifyContent: 'center',
        alignItems: 'center',
        borderRadius: 8,
        marginRight: 8,
    },
    content:{
        width: 100,
        height:116,
        backgroundColor: theme.colors.secondary40,
        borderRadius: 8,
        justifyContent: 'center',
        alignItems: 'center',
        paddingVertical: 7,
    },
    checked:{
        width:11,
        height:11,
        backgroundColor: theme.colors.primary,
        alignSelf: 'flex-end',
        marginRight: 6,
        borderRadius: 7,
        borderColor: theme.colors.secondary50,
        borderWidth: 2,

    },
    check:{
        width:11,
        height:11,
        backgroundColor: theme.colors.secondary100,
        alignSelf: 'flex-end',
        marginRight: 6,
        borderRadius: 7,
        borderColor: theme.colors.secondary50,
        borderWidth: 2,
    },
    title:{
        fontFamily: theme.fonts.title700,
        color: theme.colors.heading,
        fontSize: 15,
        marginTop: 10,
        
    }
})