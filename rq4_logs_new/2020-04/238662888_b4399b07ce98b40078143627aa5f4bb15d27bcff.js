import { StyleSheet } from 'react-native';
import { appSecondaryColor } from '../../../styles/style-config';

export default StyleSheet.create({
  overlay: {
    flexDirection: 'column',
    justifyContent: 'center',
    height: '50%',
  },
  closeModal: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'flex-end',
    paddingRight: 10,
    paddingTop: 10,
  },
  modalText: {
    flex: 4,
    paddingTop: 10,
    justifyContent: 'flex-start',
  },
  inputField: {
    width: '100%',
    marginBottom: 10,
  },
  changePW:{
    marginBottom: 10,
    marginTop: 'auto',
    backgroundColor: appSecondaryColor,
    borderWidth: 2,
    borderRadius: 15,
    paddingTop: 15,
    paddingBottom: 15,
    alignItems: 'center',
  }
});