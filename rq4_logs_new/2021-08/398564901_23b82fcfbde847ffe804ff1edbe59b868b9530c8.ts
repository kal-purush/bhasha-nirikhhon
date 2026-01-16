import Environment, { EnvironmentLocation } from 'App/Models/Environment'
import Establishment from 'App/Models/Establishment'
import Table from 'App/Models/Table'
import test from 'japa'

test.group('Environment Entity', () => {

  test('Should create a Environment', (assert) => {
    const EnvironmentEntity = new Environment()
    assert.isDefined(EnvironmentEntity)
  })

  test('Should create an Environment and manipulate his attributes', (assert) => {
    const environmentEntity = new Environment()

    environmentEntity.description = 'description'
    assert.isDefined(environmentEntity.description)

    environmentEntity.location = EnvironmentLocation.INDOOR
    assert.isDefined(environmentEntity.location)

    environmentEntity.pets_allowed = true
    assert.isDefined(environmentEntity.pets_allowed)

    environmentEntity.smoking_allowed = true
    assert.isDefined(environmentEntity.smoking_allowed)

    environmentEntity.id = 1
    assert.isDefined(environmentEntity.id)
  })

  test('Should create an Environment and create a relation with User', (assert) => {
    const environmentEntity = new Environment()
    const tableEntity = new Table()
    const establishmentEntity = new Establishment()

    environmentEntity.table = [ tableEntity ]
    assert.isDefined(environmentEntity.table)

    environmentEntity.establishment = establishmentEntity
    assert.isDefined(environmentEntity.establishment)
  })
})