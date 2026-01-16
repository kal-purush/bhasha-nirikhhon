import * as dynogels from 'drandx-dynogels';

export class BaseService {
  /**
   * return array of query result
   * @param {dynogels.Query | dynogels.Scan} query - queryset
   * @param {number} lastTimestamp - timestap from return result
   * @param {int} limit - total items returns
   */
  public result(query: dynogels.Query, limit?: number, lastTimestampItem?: number): Promise<{}> {
    return new Promise( (resolve, reject) => {
      const callback: Function = (err, resp) => {
          if (err) {
            reject(err);
          } else {
            resolve(resp.Items);
          }
      };
      if (lastTimestampItem) {
        query = query.filter('createdAt').gt(lastTimestampItem);
        if (limit) {
          limit = limit + 1;
        }
      }

      if (limit) {
        query = query.limit(limit);
      } else {
        query = query.loadAll();
      }
      query.exec(callback);
    });
  }

  /**
   * return the first element of queryset result
   * @param {dynogels.Query | dynogels.Scan} query - queryset
   */
  public first(query: dynogels.Query | dynogels.Scan): Promise<Object> {
    return new Promise( (resolve, reject) => {
      const callback: Function = (err, resp) => {
          if (err) {
            reject(err);
          } else {
            resolve(resp.Items[0]);
          }
      };
      query.limit(1);
      query.exec(callback);
    });
  }

  /**
   * return the last element of queryset result
   * @param {dynogels.Query | dynogels.Scan} query - queryset
   */
  public last(query: dynogels.Query | dynogels.Scan): Promise<Object> {
    return new Promise( (resolve, reject) => {
      const callback: Function = (err, resp) => {
          if (err) {
            reject(err);
          } else {
            resolve(resp.Items[resp.Count - 1]);
          }
      };
      query.exec(callback);
    });
  }
}