import { Fonts } from '../../Config/Constant'

export default {
  containerCentral: {
    alignItems: 'center',
    paddingLeft: 16,
    paddingRight: 28,
    marginTop: 24,
    width: 360,
    height: 64,
    paddingTop: 6,
    paddingBottom: 12,
    flexDirection: 'row'
  },
  btnBack: {
    marginRight: 32,
    width: 24,
    height: 24,
    alignItems: 'center',
    justifyContent: 'center'
  },
  iconBack: {
    fontSize: 20,
    color: 'rgba(255,255,255,1)'
  },
  containerTitle: {
    width: 260,
    flexDirection: 'column'
  },
  titlePrincipal: {
    maxWidth: 190,
    fontFamily: Fonts.SEMIBOLD,
    fontSize: 20,
    lineHeight: 24,
    marginTop: .66,
    color: 'rgba(255, 255, 255, 1)'
  },
  containerBtns: {
    width: 360,
    height: 40,
    flexDirection: 'row'
  },
  btnAnimation: {
    height: 40,
    width: 120,
    alignItems: 'center',
    backgroundColor: 'rgba(0, 0, 0, 0.05)'
  },
  circle: {
    alignItems: 'center',
    position: 'absolute',
    width: 30,
    height: 30,
    borderRadius: 15,
    backgroundColor: 'rgba(254, 254, 254, 0.3)',
    opacity: 0
  },
  txtOrder: {
    fontFamily: Fonts.SEMIBOLD,
    fontSize: 12,
    letterSpacing: 0.43,
    marginTop: 13,
    marginLeft: 30,
    color: 'rgb(255, 255, 255)',
    position: 'absolute'
  },
  breakBar1: {
    width: 1,
    height: 24,
    borderStyle: 'solid',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.3)',
    top: 8,
    left: 119,
    position: 'absolute'
  },
  txtFilter: {
    fontFamily: Fonts.SEMIBOLD,
    fontSize: 12,
    textAlign: 'center',
    letterSpacing: 0.43,
    marginTop: 13,
    marginLeft: 15,
    color: 'rgba(255, 255, 255, 1)',
    position: 'absolute'
  },
  breakBar2: {
    width: 1,
    height: 24,
    borderStyle: 'solid',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.3)',
    top: 8,
    left: 238,
    position: 'absolute'
  },
  txtMap: {
    fontFamily: Fonts.SEMIBOLD,
    fontSize: 12,
    textAlign: 'center',
    letterSpacing: 0.43,
    marginTop: 13,
    marginLeft: 15,
    color: 'rgba(255, 255, 255, 1)',
    position: 'absolute'
  }
}