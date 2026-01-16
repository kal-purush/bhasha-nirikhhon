import { StyleSheet } from 'react-native';

export const EnterMobileNumberStyle =(themeColors:any)=> StyleSheet.create({
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
    height: '56%',
    width: '90%',
    alignSelf: 'center',
  },
  button: {
    position: 'absolute',
    bottom: 0,
    width: '100%'
  },
});