import { StyleSheet } from 'react-native';

export const homeStyle = StyleSheet.create({
    container: {
        flex: 1,
        justifyContent: 'center',
        alignContent:'center',
    },
    header: {
        flex:1,
        justifyContent:'center',
        alignItems:'center'
    },
    searchBar: {
        backgroundColor:'rgba(0, 0, 0, 0.01)',
        width: '50%',
        height: 30,
        shadowColor:'rgba(0, 0, 0, 0.2)',
        shadowOffset:{width:0, height:2},
        shadowOpacity:0.8,
        shadowRadius:2,
        elevation:1
    },
    body: {
        alignItems:'center'
    },
    card: {
        width: '90%',
        height: 180,
        backgroundColor:'rgba(0, 0, 0, 0.01)',
        shadowColor:'rgba(0, 0, 0, 0.2)',
        shadowOffset:{width:0, height:2},
        shadowOpacity:0.8,
        shadowRadius:2,
        elevation:1,
        marginBottom:25
    }
});