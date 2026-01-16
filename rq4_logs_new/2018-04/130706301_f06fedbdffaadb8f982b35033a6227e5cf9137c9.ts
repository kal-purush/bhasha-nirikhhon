import { Fonts } from '../../Config/Constant'

export default {
  container: {
    backgroundColor: 'rgb(248, 248, 248))',
    width: 360,
    height: 640,
    flexDirection: 'row'
  },
  iconFlight: {
    fontSize: 23,
    marginTop: 60,
    marginLeft: 169,
    position: 'absolute',
    transform: [{
      rotateZ: '90deg'
    }]
  },
  containerDateStart: {
    width: 328,
    height: 40,
    marginTop: 300,
    marginLeft: 16,
    flexDirection: 'row',
    backgroundColor: 'rgb(255, 255, 255)',
    borderRadius: 2,
    elevation: 3,
    position: 'absolute'
  },
  containerDateEnd: {
    width: 328,
    height: 40,
    marginTop: 360,
    marginLeft: 16,
    flexDirection: 'row',
    backgroundColor: 'rgb(255, 255, 255)',
    borderRadius: 2,
    elevation: 3,
    position: 'absolute'
  },
  iconCalendar: {
    fontSize: 19,
    marginTop: 10,
    marginLeft: 8
  },
  dateDeparture: {
    fontFamily: Fonts.REGULAR,
    color: 'rgb(0, 0, 0)',
    marginLeft: 100
  },
  containerBtn: {
    width: 328,
    height: 40,
    marginTop: 500,
    marginLeft: 16,
    backgroundColor: 'rgb(234, 100, 34)',
    borderRadius: 2,
    elevation: 3,
    position: 'absolute'
  },
  btnSearch: {
    fontFamily: Fonts.SEMIBOLD,
    color: 'rgb(255, 255, 255)',
    marginTop: 12,
    marginLeft: 140
  }
}