import { Operation } from '../redux/utils'
import axios from 'axios'
import { assign } from './../redux/utils'

export class fetchMateriais extends Operation {
  public process = async (getState, dispatch, done) => {
    const { data } = await axios({
      method: 'get',
      url: '/search?Query=teste&ContentTypeIds%5B%5D=1&PageNumber=0&PageSize=20&Order=2',
      headers: {
        'Access-Control-Allow-Origin': '*'
      }
    })
    console.log(`data`, data)
    dispatch(new fetchMateriaisSuccess(data))
  }
}

export class fetchMateriaisSuccess extends Operation {
  constructor(public payload) {
    super()
  }

  public containerReducer(state) {
    return assign(state, { materias: this.payload })
  }
}