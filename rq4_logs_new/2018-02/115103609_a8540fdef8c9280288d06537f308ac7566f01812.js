import oracledb from 'oracledb';
import oracleService from '../../config/application';

class connectDynamicOracle {

  constructor() {
    this.connectionAttrs = {
      user: oracleService.DynamicObjUserName,
      password: oracleService.DynamicObjPassword,
      connectString: oracleService.connectDynamicObjUrl
    };
    this.releaseConnections = this.releaseConnections.bind(this);
  }

  executeSql (sql){
    let _self = this;
    oracledb.autoCommit = true;
    return new Promise((resolve, reject) => {
      oracledb
        .getConnection(this.connectionAttrs)
        .then(connection => {
          return connection
            .execute(sql)
            .then(results => {
              resolve(results);
              this.releaseConnections(connection);
            })
            .catch(err => {
              reject(err);
              this.releaseConnections(connection)
            })
        })
        .catch(err => {
          reject(err)
        })
    })}

  releaseConnections(resultSet, connection){
    process.nextTick(() => {
      if (resultSet) {
        resultSet
          .close()
          .then(() => {
            connection
              .release()
              .catch(err => {
                throw err
              })
          })
          .catch(err => {
            throw err
          })
      } else {
        connection
          .release()
          .catch(err => {
            throw err
          })
      }
    })
  }
}

export default connectDynamicOracle;