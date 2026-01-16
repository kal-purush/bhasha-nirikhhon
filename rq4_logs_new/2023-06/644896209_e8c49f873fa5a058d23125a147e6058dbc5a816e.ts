import { StyleSheet, Dimensions } from 'react-native';
import { moderateScale } from '@Theme/Metrics';
const { width, height } = Dimensions.get('window');
const modalWidth = width * 0.9;
const modalHeight = height * 0.5;

export const CountryDialogStyle = StyleSheet.create({
  modalContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
  },
  container: {
    width: modalWidth,
    height: modalHeight,
    backgroundColor: 'white',
    borderRadius: moderateScale(8),
    padding: moderateScale(16),
  },
  searchBarContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    borderBottomWidth: moderateScale(1),
    borderBottomColor: 'gray',
    marginBottom: moderateScale(16),
  },
  searchIcon: {
    marginRight: moderateScale(8),
  },
  searchInput: {
    flex: 1,
  },
  itemContainer: {
    paddingVertical: moderateScale(8),
  },
  closeButton: {
    alignSelf: 'flex-end',
  },
  item: { flex: 1, flexDirection: 'row', alignItems: 'center' },
  flag: { flex: 0.12 },
  code: { flex: 0.2 },
  name: { flex: 0.68 },
});