import { Text , StyleSheet, Pressable} from "react-native"
const item = ({id, nome, onDeleteProduct}) => {
    return(
        <Pressable onPress={() => onDeleteProduct(id)}>
        <Text style={styles.lista}>{id} - {nome}</Text>
        </Pressable>
    )
}
const styles = StyleSheet.create({
    lista:{
        fontSize:14,
        lineHeight: 24,
        fontStyle:"italic",
        fontWeight: "bold",

    }
})

export default item;