
import Environment from 'App/Models/Environment'
import EnvironmentRepository from 'App/Repository/EnvironmentRepository'
import EnvironmentValidator from 'App/Validator/EnvironmentValidator'

import CrudController from './Base/CrudController'

export default class EnvironmentController extends CrudController<
  EnvironmentValidator,
  Environment,
  EnvironmentRepository
>{

  constructor() {
    super(
      new EnvironmentValidator(),
      new Environment(),
      new EnvironmentRepository()
    )
  }

}
