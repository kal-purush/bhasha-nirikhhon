import { colors } from '@src/theme/colors';
import { StyleSheet } from 'react-native';
const itemHeight = 44;
const itemHorizontalMargin = 5;

export const ChooseAccountTypeStyle = StyleSheet.create({
  button: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#efefef',
    height: 50,
    zIndex: 1,
  },
  buttonText: {
    flex: 1,
    textAlign: 'center',
  },
  icon: {
    marginRight: 10,
  },
  informationLogoContainer: { 
    right: 0, 
    justifyContent: 'center' 
  },
  container: {
    height: (itemHeight + (itemHorizontalMargin * 2)) * 4,
    shadowColor: '#000000',
    shadowRadius: 1,
    shadowOffset: { height: 2, width: 0 },
    shadowOpacity: 0.2,
  },
  overlay: {
    width: '100%',
    height: '100%',
  },
  item: {
    justifyContent: 'center',
    alignItems: 'center',
    flexDirection: 'row',
    backgroundColor: colors.light.chooseActionItemBackground,
    paddingHorizontal: 10,
    paddingVertical: 10,
    borderWidth: 1,
    marginHorizontal: 16,
    height: itemHeight,
    borderRadius: 4,
    marginVertical: 5,
  },
  text: {
    color: '#F5F6F7',
  },
});