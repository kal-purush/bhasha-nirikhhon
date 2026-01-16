import { MessageData } from '../../assets/Types';

export class ValidExpoPushClient {

    async pushToClients(recipients: string[], data: MessageData): Promise<any>{
        return Promise.resolve('done')
    }
}