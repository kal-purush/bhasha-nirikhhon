import { StyleSheet } from "react-native";

export const FormStyles = StyleSheet.create({
  form: {
    position: "relative",
    paddingTop: 92,
    paddingBottom: 78,
    backgroundColor: "#FFFFFF",
    borderTopLeftRadius: 25,
    borderTopRightRadius: 25,
  },
  enterForm: {
    paddingTop: 32,
  },
  photoBox: {
    position: "absolute",
    top: -60,
    width: 120,
    height: 120,
    backgroundColor: "#F6F6F6",
    borderRadius: 16,
  },
  photo: {
    width: 120,
    height: 120,
    borderTopLeftRadius: 16,
    borderTopRightRadius: 16,
  },
  photoIcon: {
    position: "absolute",
    top: 81,
    left: 107,
  },
  textTitleForm: {
    fontFamily: "Roboto-Medium",
    fontSize: 30,
    marginBottom: 33,
    textAlign: "center",
  },
  input: {
    fontFamily: "Roboto-Regular",
    marginHorizontal: 16,
    marginBottom: 16,
    paddingLeft: 16,
    height: 50,
    borderWidth: 1,
    borderColor: "#E8E8E8",
    borderRadius: 8,
    backgroundColor: "#F6F6F6",
    color: "#212121",
  },
  inputFocused: {
    borderColor: "#FF6C00",
  },
  passwordBox: {
    position: "relative",
  },
  passwordBtn: {
    position: "absolute",
    top: 16,
    right: 32,
  },
  buttonContainer: {
    marginHorizontal: 16,
  },
  buttonReg: {
    marginHorizontal: 16,
    backgroundColor: "#FF6C00",
    height: 51,
    marginTop: 27,
    borderRadius: 100,
    justifyContent: "center",
    alignItems: "center",
  },
  btnTitleReg: {
    fontFamily: "Roboto-Regular",
    fontWeight: 400,
    fontSize: 16,
    color: "#FFFFFF",
    fontSize: 16,
  },
  buttonEnt: {
    marginHorizontal: 16,
    height: 51,
    borderRadius: 100,
    justifyContent: "center",
    alignItems: "center",
  },
  btnTitleEnt: {
    fontFamily: "Roboto-Regular",
    fontWeight: 400,
    fontSize: 16,
    color: "#1B4371",
    fontSize: 16,
  },
});

// export default FormStyles;