import { StyleSheet, Text, View } from "react-native";

export const EmptyList = () => {

  return (
    <View style={styles.view}>
      <Text style={styles.text}>No search results</Text>
    </View>
  )
}

const styles = StyleSheet.create({
  view: {
    width: '100%',
    height: '100%',
    display: 'flex',
    alignItems:'center',
  },
  text: {
    position: 'relative',
    fontSize: 16,
    fontWeight: '500',
  },
});