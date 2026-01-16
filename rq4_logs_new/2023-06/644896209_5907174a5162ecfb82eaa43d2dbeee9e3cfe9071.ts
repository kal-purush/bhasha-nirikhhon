import { StyleSheet } from 'react-native';

export const EnterEmailStyle = StyleSheet.create({
  container: {
    position: 'absolute',
    bottom: 0,
    height: '100%',
    width: '100%',
    flexDirection: 'column',
  },
  containerSpace: {
    alignItems: 'center',
    height: '12%',
  },
  containerLogo: {
    alignItems: 'center',
    height: '25%',
  },
  containerInput: {
    height: '58%',
    width: '90%',
    alignSelf: 'center',
  },
  textMargin: {
    marginTop: 15,
  },
  button: {
    position: 'absolute',
    bottom: 0,
    backgroundColor: 'yellow',
    width: '100%',
    height: 38,
  },
});