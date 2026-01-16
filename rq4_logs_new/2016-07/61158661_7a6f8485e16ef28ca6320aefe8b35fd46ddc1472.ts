import * as Server from './server';
import * as DTOsNamespace from './dtos/_namespace';
import * as ManagersNamespace from './managers/_namespace';
import * as ModelsNamespace from './models/_namespace';
import * as RepositoriesNamespace from './repositories/_namespace';
import * as SerializersNamespace from './serializers/_namespace';
import * as ServicesNamespace from './services/_namespace';

exports = {

    Server: Server,
    DTOs: DTOsNamespace,
    Managers: ManagersNamespace,
    Models: ModelsNamespace,
    Repositories: RepositoriesNamespace,
    Serializers: SerializersNamespace,
    Services: ServicesNamespace

};