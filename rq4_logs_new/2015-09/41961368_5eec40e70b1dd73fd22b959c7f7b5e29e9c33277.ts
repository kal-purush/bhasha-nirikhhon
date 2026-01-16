///<reference path='../interfaces/IDataChannel.ts'/>
///<reference path='../interfaces/IRequest.ts'/>
///<reference path='../interfaces/IResponse.ts'/>
///<reference path='../interfaces/IRecord.ts'/>

import { Constants } from '../interfaces/Constants';

export class StreamHandler {
    private dataChannel: IDataChannel;

    constructor(dataChannel?: IDataChannel) {
        this.dataChannel = dataChannel;
    }

    processRequest(request: IRequest): IResponse {
        switch (request.command) {
            case Constants.COMMAND_IDS:
                return this.processCommandIds();
            case Constants.COMMAND_READ:
                return this.processRead(request.id);
        }

        return StreamHandler.createError("Unknown command");
    }

    private static createError(message: string): IResponse {
        var ret: IResponse = <any>{};
        ret.error = message;
        return ret;
    }

    private processCommandIds(): IResponse {
        var ret: IResponse = <any>{};
        ret.ids = this.dataChannel.getIds();
        return ret;
    }

    private processRead(id: string): IResponse {
        var ret: IResponse = <any>{};
        ret.record = this.dataChannel.getRecord(id);
        return ret;
    }
}