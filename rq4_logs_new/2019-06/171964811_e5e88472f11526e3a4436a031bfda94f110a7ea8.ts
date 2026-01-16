import { Types } from 'mongoose';
declare namespace Credential {
    interface Model {
        _id?: Types.ObjectId;
        clubID?: Types.ObjectId;
        branch?: Api;
        oneSignal?: Api;
        googleCal?: OAuth;
        iCal?: OAuth;
    }
    interface Api {
        devAppID?: string;
        devApiKey?: string;
        devSecretKey?: string;
        prodAppID?: string;
        prodApiKey?: string;
        prodSecretKey?: string;
    }
    interface OAuth {
        refreshToken?: string;
        accessToken?: string;
        secret?: string;
        link?: string;
    }
}
export default Credential;