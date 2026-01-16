import { StyleSheet, Platform} from "react-native";

export const adminStyle = StyleSheet.create({
    container: {
        marginTop: 20,
    },
    header: {
        flex: 1,
        backgroundColor: 'powderblue',
        padding: 30,
        alignItems: 'center',
    },
    text:{
        fontSize: 30
    },
    addRoom: {
        flex: 2,
        backgroundColor: 'blue',
        padding: 50
    },
    manageRoles: {
        flex: 3,
        backgroundColor:'powderblue',
        padding: 50
    }
  
})