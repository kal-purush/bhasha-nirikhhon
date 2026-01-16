import APIResource from '../APIResource'
import APIList from '../APIList'

/**
 * InputFileset
 *
 * @class
 * @extends APIResource
 */
class InputFileset extends APIResource {

  /**
   * /input-file-set
   *
   * Constructor
   */
  constructor (parent: object) {
    super(parent);
    this.push('input-file-set');
  }

  // /input-file-set/files
  public files () {
    return new APIList(this).push('files');
  }

  // /input-file-set/files.zip
  public filesZip () {
    return new APIResource(this).push('files.zip');
  }
}

export default InputFileset