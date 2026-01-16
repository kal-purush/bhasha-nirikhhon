import {
  Count,
  CountSchema,
  Filter,
  repository,
  Where,
} from '@loopback/repository';
import {
  del,
  get,
  getModelSchemaRef,
  getWhereSchemaFor,
  param,
  patch,
  post,
  requestBody,
} from '@loopback/rest';
import {
  Cliente,
  DireccionCliente,
} from '../models';
import {ClienteRepository} from '../repositories';

export class ClienteDireccionClienteController {
  constructor(
    @repository(ClienteRepository) protected clienteRepository: ClienteRepository,
  ) { }

  @get('/clientes/{id}/direccion-cliente', {
    responses: {
      '200': {
        description: 'Cliente has one DireccionCliente',
        content: {
          'application/json': {
            schema: getModelSchemaRef(DireccionCliente),
          },
        },
      },
    },
  })
  async get(
    @param.path.number('id') id: number,
    @param.query.object('filter') filter?: Filter<DireccionCliente>,
  ): Promise<DireccionCliente> {
    return this.clienteRepository.direccionCliente(id).get(filter);
  }

  @post('/clientes/{id}/direccion-cliente', {
    responses: {
      '200': {
        description: 'Cliente model instance',
        content: {'application/json': {schema: getModelSchemaRef(DireccionCliente)}},
      },
    },
  })
  async create(
    @param.path.number('id') id: typeof Cliente.prototype.id,
    @requestBody({
      content: {
        'application/json': {
          schema: getModelSchemaRef(DireccionCliente, {
            title: 'NewDireccionClienteInCliente',
            exclude: ['id'],
            optional: ['id_cliente']
          }),
        },
      },
    }) direccionCliente: Omit<DireccionCliente, 'id'>,
  ): Promise<DireccionCliente> {
    return this.clienteRepository.direccionCliente(id).create(direccionCliente);
  }

  @patch('/clientes/{id}/direccion-cliente', {
    responses: {
      '200': {
        description: 'Cliente.DireccionCliente PATCH success count',
        content: {'application/json': {schema: CountSchema}},
      },
    },
  })
  async patch(
    @param.path.number('id') id: number,
    @requestBody({
      content: {
        'application/json': {
          schema: getModelSchemaRef(DireccionCliente, {partial: true}),
        },
      },
    })
    direccionCliente: Partial<DireccionCliente>,
    @param.query.object('where', getWhereSchemaFor(DireccionCliente)) where?: Where<DireccionCliente>,
  ): Promise<Count> {
    return this.clienteRepository.direccionCliente(id).patch(direccionCliente, where);
  }

  @del('/clientes/{id}/direccion-cliente', {
    responses: {
      '200': {
        description: 'Cliente.DireccionCliente DELETE success count',
        content: {'application/json': {schema: CountSchema}},
      },
    },
  })
  async delete(
    @param.path.number('id') id: number,
    @param.query.object('where', getWhereSchemaFor(DireccionCliente)) where?: Where<DireccionCliente>,
  ): Promise<Count> {
    return this.clienteRepository.direccionCliente(id).delete(where);
  }
}