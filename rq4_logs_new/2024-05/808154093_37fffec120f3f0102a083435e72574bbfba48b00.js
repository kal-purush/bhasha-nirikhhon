import { StatusBar } from "expo-status-bar";
import { StyleSheet, Text, View } from "react-native";

export default function App() {
  return (
    <View style={styles.container}>
      <View style={styles.container1}>
        <Text style={styles.text1}>Hello , Devs</Text>
        <Text style={styles.text2}>14 Tasks today</Text>
        <View style={styles.round}></View>
      </View>
      <View style={styles.container2}></View>
      <StatusBar style="auto" />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#F7F0E8",
    alignItems: "center",
    justifyContent: "center",
  },
  container1: {
    flexDirection: "row",
    height: "",
    width: "90%",
    backgroundColor: "pink",
    paddingHorizontal: 2,
    marginBottom: 20,
    alignItems: "center",
    justifyContent: "space-between",
    flexWrap: "",
  },
  text1: {
    fontSize: 40,
    fontWeight: "normal",
    textAlign: "left",
    marginBottom: 50,
  },
  text2: {
    fontSize: 12,
    fontWeight: "normal",
    textAlign: "left",
    marginTop: 20,
    marginLeft: -330,
  },
  round: {
    backgroundColor: "white",
    width: 70,
    height: 70,
    opacity: 0.4,
    borderRadius: 40,
    marginLeft: 100,
  },
  container2: {
    flexDirection: "row",
    height: "10%",
    width: "90%",
    marginBottom: 600,
    backgroundColor: "yellow",
    alignItems: "center",
    justifyContent: "space-between",
    flexWrap: "",
  },
});