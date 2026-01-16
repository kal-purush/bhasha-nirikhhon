import { RequestManager } from '../../managers/requestManager';
import { Injectable } from '@angular/core';
import { map } from 'rxjs/operators';
import { PopUpManager } from '../../managers/popUpManager';

@Injectable({
    providedIn: 'root',
})
export class ComprobanteHelper {

    query_params: String;
    constructor(
        private rqManager: RequestManager,
        private pUpManager: PopUpManager,
    ) { }
    /**
       * getComprobantes
       * If the response has errors in the OAS API it should show a popup message with an error.
       * If the response suceed, it returns the data of the object.
       * @param id object to get in the DB
       * @param param object with the params to get in the DB
       * @returns  <Observable> data of the object registered at the DB. undefined if the request has errors
       */
    public getComprobantes(id?: any, params?: any) {
        this.rqManager.setPath('CUENTAS_CONTABLES_SERVICE');
        return this.rqManager.get('comprobante/' + id).pipe(
            map(
                (res) => {
                    if (res === 'error') {
                        this.pUpManager.showErrorAlert('No se pudo consultar los tipos de comprobante');
                        return undefined;
                    }
                    return res;
                },
            ),
        );

    }
    /**
       * comprobanteRegister
       * If the response has errors in the OAS API it should show a popup message with an error.
       * If the response suceed, it returns the data of the updated object.
       * @param data object to save in the DB
       * @returns  <Observable> data of the object registered at the DB. undefined if the request has errors
       */
    public comprobanteRegister(data) {
        this.rqManager.setPath('CUENTAS_CONTABLES_SERVICE');
        return this.rqManager.post(`comprobante/`, data).pipe(
            map(
                (res) => {
                    if (res['Type'] === 'error') {
                        this.pUpManager.showErrorAlert(res['Message']);
                        return undefined;
                    }
                    return res;
                },
            ),
        );

    }

    /**
     * comprobanteUpdate
     * If the response has errors in the OAS API it should show a popup message with an error.
     * If the response is successs, it returns the data of the updated object.
     * @param data fields to update
     * @returns  <Observable> object updated information. undefined if the proccess has errors.
     */
    public comprobanteUpdate(data) {
        this.rqManager.setPath('CUENTAS_CONTABLES_SERVICE');
        return this.rqManager.put('comprobante/', data, data._id).pipe(
            map(
                (res) => {
                    if (res['Type'] === 'error') {
                        this.pUpManager.showErrorAlert('No se pudo actualizar el tipo de comprobante');
                        return undefined;
                    }
                    return res;
                },
            ),
        );

    }

    /**
     * comprobanteDelete
     * If the response has errors in the OAS API it should show a popup message with an error.
     * If the response is successs, it returns the data of the updated object.
     * @param id the id of the object to remove in the DB.
     * @returns  <Observable> object with api response. undefined if the proccess has errors
     */
    public comprobanteDelete(id: number, params?: any) {
        this.rqManager.setPath('CUENTAS_CONTABLES_SERVICE');
        return this.rqManager.delete('comprobante', id.toString()).pipe(
            map(
                (res) => {
                    return res;
                },
            ),
        );

    }

}