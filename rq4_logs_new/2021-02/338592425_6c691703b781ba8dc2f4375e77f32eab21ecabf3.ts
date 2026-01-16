import { CallbackError } from 'mongoose';
import ChargePoint, { IChargePoint, IChargePointDocument } from '../models/charge-point.model';

class ChargePointService {

    public saveChargePoint(chargePoint: IChargePoint): Promise<IChargePoint> {
        return new Promise((resolve, reject) => {
            new ChargePoint(chargePoint).save()
                .then((doc: IChargePointDocument) => resolve(doc))
                .catch((err: CallbackError) => reject({status: 500, message: err?.message}));
        })
    }

}

export default ChargePointService;