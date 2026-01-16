import { JsonConverter } from './json-converter';
import { Client } from '../../../_model/person/client.model';
import { SerializationHelper } from '../serialization.helper';

export class ClientJsonConverter implements JsonConverter<Client> {

  private static instance: ClientJsonConverter = null;

  constructor() {}

  public static getInstance(): ClientJsonConverter {
    if (this.instance === null) {
      this.instance = new ClientJsonConverter();
    }
    return this.instance;
  }


  convertJsonToObjectsArray(_data: any[]): Client[] {

    const clients: Client[] = new Array();

    for (let j = 0; j < _data.length; j = j + 1) {
      clients.push(
        SerializationHelper.toInstance(new Client(),
          JSON.stringify(_data[j]))
      );

    }
    return clients;
  }
}