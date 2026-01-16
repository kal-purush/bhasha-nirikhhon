import APIResource from './APIResource'
import APIList from './APIList'


/**
 * APIAdminResourceCluster
 *
 * @class
 * @extends APIResource
 */
class APIAdminResourceCluster extends APIResource {

  /**
   * /clusters/{id}
   *
   * Constructor
   */
  constructor (parent: object, id: number) {
    if (id == null) {
      throw new Error('Resource ID cannot be null!');
    }

    super(parent);
    this.push('clusters', id);
  }

  // /clusters/{id}/devices
  public devices () {
    return new APIList(this).push('devices');
  }

}

export default APIAdminResourceCluster