import { Injectable } from '@angular/core';
import { map } from 'rxjs';
import { IReqApi } from 'src/libs/common/interface/interfaces';
import { BaseRequestAbstractService } from 'src/libs/service/request/abstract-api.service';
import { BaseRequestService } from 'src/libs/service/request/base-request.service';
@Injectable({ providedIn: 'root' })

export class CheckoutService {
    constructor(private abstractService: BaseRequestService) { }

    checkout(body:any): Promise<any> {

            return new Promise<any>((resolve, reject) => {
                this.abstractService.post('invoice/online/payment',body)
                .subscribe(
                    (result) => {
                        console.log(result);
                        
                        return resolve(result)
                    },
                    (err) => reject(err)
                );
            });
        
        
    }

    findByMaPhieuGiamGia(params?: any): Promise<any> {
        return new Promise<any>((resolve, reject) => {
            this.abstractService.get('voucher/code',params).subscribe(
                (result) => {
                    return resolve(result)
                },
                (err) => reject(err)
            );
        });
    }
}