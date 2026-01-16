import {rules, schema} from '@ioc:Adonis/Core/Validator'
import type { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'

export default class CrearAlumnoPersonaValidator {
  constructor(protected ctx: HttpContextContract) {}
  public schema = schema.create({
    codigo: schema.string({}, [
      rules.required(),
      rules.unique({table: 'alumno', column: 'codigo'}),
      rules.maxLength(12)
    ]),
    proceso_admision_id: schema.string({}, [
      rules.required(),
      rules.exists({table: 'proceso_admision', column: 'id'})
    ]),
    estado: schema.string({}, [
      rules.required(),
      rules.maxLength(1)
    ]),
  })
  public messages = {
    'codigo.required':                  'Se requiere un código de alumno',
    'codigo.unique':                    'Se requiere un código de alumno distinto',
    'codigo.maxLength':                 'Se requiere un código de alumno de 12 caracteres o menos de tamaño',
    'proceso_admision_id.required':     'Se requiere un id de proceso de admisión ',
    'proceso_admision_id.exists':       'Se requiere un id de proceso de admisión existente',
    'estado.required':                  'Se requiere un estado de alumno',
    'estado.maxLength':                 'Se requiere un estado de alumno de 1 caracter',

  }
}