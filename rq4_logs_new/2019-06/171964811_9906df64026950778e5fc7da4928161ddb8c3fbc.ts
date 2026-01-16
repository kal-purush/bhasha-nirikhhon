import { Types } from 'mongoose';
import Location from './subModels/shared/location';
import IShared from './shared';
declare namespace Card {
    interface Model {
        _id?: Types.ObjectId;
        clubID?: Types.ObjectId;
        userID?: Types.ObjectId;
        remoteID?: string;
        holderName?: string;
        location?: Location.Model;
        brand?: Brand;
        cvcCheck?: CVCCheck;
        last4?: number;
        expireMonth?: number;
        expireYear?: number;
        funding?: Funding;
        metadata?: IShared.GeneralMap<any>;
        fingerprint?: string;
    }
    enum Brand {
        AmericanExpress = "American Express",
        DinersClub = "Diners Club",
        Discover = "Discover",
        JCB = "JCB",
        MasterCard = "Master Card",
        UnionPay = "Union Pay",
        Visa = "Visa",
        Unknown = "Unknown"
    }
    enum Funding {
        Credit = "Credit",
        Debit = "Debit",
        Prepaid = "Prepaid",
        Unknown = "Unknown"
    }
    enum CVCCheck {
        Pass = "Pass",
        Fail = "Fail",
        Unavailable = "Unavailable",
        Unchecked = "Unchecked"
    }
}
export default Card;