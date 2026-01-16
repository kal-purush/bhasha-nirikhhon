import { StyleSheet } from 'react-native';
import colors from '../../utils/colors';
import { deviceWidth } from '../../utils/commons';

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: colors.RED
  },
  container: {
    backgroundColor: colors.RED
  },
  contentContainer: {
    flex: 1,
    backgroundColor: colors.RED,
    alignItems: 'center',
    justifyContent: 'center'
  },
  logo: {
    width: 105,
    height: 53,
    resizeMode: 'contain'
  },
  logoText: {
    marginTop: 18,
    color: colors.WHITE,
    fontSize: 14,
    letterSpacing: 0.07,
    marginBottom: 57
  },
  inputContainer: {
    padding: 15,
    width: deviceWidth,
    height: 65
  },
  label: {
    color: colors.WHITE,
    fontSize: 12
  },
  input: {
    color: colors.WHITE,
    borderBottomWidth: 0.5,
    borderBottomColor: colors.WHITE,
    padding: 5,
    fontSize: 12
  },
  forgotButton: {
    height: 50,
    width: deviceWidth,
    padding: 15,
    alignItems: 'flex-end'
  },
  forgotText: {
    color: colors.WHITE,
    fontSize: 12
  },
  loginButton: {
    height: 40,
    width: deviceWidth - 30,
    backgroundColor: colors.WHITE,
    borderRadius: 10,
    alignItems: 'center',
    justifyContent: 'center'
  },
  loginButtonText: {
    color: colors.BLACK,
    fontSize: 14,
    fontWeight: 'bold'
  },
  registerButton: {
    height: 40,
    width: deviceWidth - 30,
    alignItems: 'center',
    justifyContent: 'center'
  },
  registerButtonText: {
    color: colors.WHITE,
    fontSize: 14,
    fontWeight: 'bold'
  }
});

export default styles;