import baseService from 'modules/qcrud/_services/baseService'

export default {
  getDevices(){
    return new Promise((resolve, reject) => {
      baseService.index('apiRoutes.qtelemetry.devices', {refresh: true}).then(response => {
        resolve(response.data)
      }).catch(error => reject(error));
    })
  }
}