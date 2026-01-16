import {Model, model, property} from '@loopback/repository';

@model()
export class SolicitarViaje extends Model {
  @property({
    type: 'number',
  })
  idCliente?: number;

  @property({
    type: 'number',
  })
  idConductor?: number;

  @property({
    type: 'number',
  })
  destino?: number;


  constructor(data?: Partial<SolicitarViaje>) {
    super(data);
  }
}

export interface SolicitarViajeRelations {
  // describe navigational properties here
}

export type SolicitarViajeWithRelations = SolicitarViaje & SolicitarViajeRelations;