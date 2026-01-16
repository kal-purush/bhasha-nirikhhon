import { Injectable } from '@angular/core';
import { ServerCommunicationService } from 'src/app/server-communication/server-communication.service'; 
import { Instance } from '../model/instance/instance.model';


@Injectable({
  providedIn: 'root'
})
export class InstanceService {
    private instancesPath: string = 'instances/';

    constructor(private serverCommunicationService: ServerCommunicationService) { }
    
    public async getInstanceWithRelatedInstances(id: number): Promise<Instance> {
        return await this.serverCommunicationService.getRequestAsync(this.instancesPath + id + '/related-instances');
    }

    public async getInstanceWithAnnotations(id: number): Promise<Instance> {
        return await this.serverCommunicationService.getRequestAsync(this.instancesPath + id + '/annotations');
    }
}