// 此文件是由模板文件 ".dtpl/page/$rawModuleName.ts.dtpl" 生成的，你可以自行修改模板

import {pagify, MyPage} from 'base/'

@pagify()
export default class extends MyPage {
  data = {
    address: ''
  }

  async onLoad(options: any) {
    // console.log(await wxp.getUserInfo())
    // let that:any = this
    let store:any = this.store
    this.setDataSmart({
      address: store.address
    })
  }
}