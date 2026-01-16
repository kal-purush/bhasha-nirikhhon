import { StyleSheet } from 'react-native';
import colors from '../../utils/colors';
import width from '../../utils/commons';

const styles = StyleSheet.create({
  container: {
    width: width.deviceWidth,
    height: 180,
    paddingLeft: 16,
    paddingRight: 16,
    flexDirection: 'row'
  },
  imageContainer: {
    marginRight: 16,
  },
  image: {
    width: 140,
    height: 140,
    resizeMode: 'contain'
  },
  contentContainer: {
    flex: 1,
    height: 140,
    justifyContent: 'space-between'
  },
  subTitle: {
    fontSize: 12,
    color: colors.BLACK
  },
  title: {
    fontSize: 14,
    color: colors.BLACK,
    fontWeight: '600'
  },
  button: {
    backgroundColor: colors.RED,
    borderColor: colors.RED
  },
  starContainer: {
    width: 112
  },
  star: {
    marginRight: 5
  }
});

export default styles;