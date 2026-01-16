declare global {
  interface Window {
    CloudConnect: any;
  }
}

export default class ConnectManager {
  public CloudConnect: any;

  public initialize(callback?: any) {
    window.addEventListener("scriptLoaded", async (event) => {
      this.CloudConnect = window.CloudConnect;
      if (callback && typeof(callback) === 'function') {
        callback();
      }
    });
  }

  public authenticate(callback?: any) {
    if (this.CloudConnect) {
        this.CloudConnect.getPublicKey().then((res: any) => {
          const { publicKey, success, error } = res;
          if (success) {
            callback(publicKey);
          } else {
            console.log(error);
            callback(null);
          }
        }).catch((error: any) => {
          console.log(error);
          callback(null);
        });
    } else {
      // confirm('You have to install AIN Connect plugin.')
    }
  }

  public encrypt(data: Array<number>, callback?: any) {
    if (this.CloudConnect) {
      this.CloudConnect.encryptData(data).then((res: any) => {
        callback(res);
      }).catch((error: any) => {
        console.log(error);
        callback(null);
      })
    } else {
      // confirm('You have to install AIN Connect plugin.')
    }
  }

  public decrypt(data: string, callback?: any) {
    if (this.CloudConnect) {
      this.CloudConnect.decryptData(data).then((res: any) => {
        callback(res);
      }).catch((error: any) => {
        console.log(error);
        callback(null);
      })
    } else {
      // confirm('You have to install AIN Connect plugin.')
    }
  }
}