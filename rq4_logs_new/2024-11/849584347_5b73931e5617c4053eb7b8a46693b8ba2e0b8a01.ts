import { EventosService } from '../service/eventos.service';
import { EventosDocument } from 'src/todos/document/eventos.document';
declare const GenericEventosController: {
    new (): {
        readonly genericService: import("../shared/generic.service").GenericService<EventosDocument>;
        findAll(): Promise<EventosDocument[]>;
        findById(id: string): Promise<EventosDocument>;
        create(data: EventosDocument): Promise<EventosDocument>;
        update(id: string, data: Partial<EventosDocument>): Promise<void>;
        delete(id: string): Promise<void>;
    };
};
export declare class EventosController extends GenericEventosController {
    private readonly eventosService;
    constructor(eventosService: EventosService);
    getEventosByEstado(estado: 'aceptado' | 'reechazado' | 'pendiente'): Promise<EventosDocument[]>;
    getEventosProximos(fechaBase: string): Promise<EventosDocument[]>;
    getEventosByCliente(idCliente: string): Promise<EventosDocument[]>;
}
export {};