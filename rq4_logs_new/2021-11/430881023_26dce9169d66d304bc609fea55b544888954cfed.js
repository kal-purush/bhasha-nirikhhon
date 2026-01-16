import { StyleSheet, Dimensions } from "react-native";

const { width } = Dimensions.get("window");

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "#7FB800",
  },
  TextInputBox: {
    borderRadius: 10,
    borderWidth: 1,
    height: 40,
    margin: 12,
    padding: 10,
    backgroundColor: "white",
    width: width * 0.8,
  },
});

export default styles;