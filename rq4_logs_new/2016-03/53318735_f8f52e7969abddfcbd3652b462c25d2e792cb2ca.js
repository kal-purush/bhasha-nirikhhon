import Model from './Model';

/*
contacts
  - userId
    - username: true
    - username: true
*/
export default class Contacts extends Model {
  constructor(model = 'contacts', adapter) {
    super(model, adapter);
  }       
}