import { Types } from 'mongoose';
declare namespace Lottery {
    interface Model {
        _id?: Types.ObjectId;
        clubID: Types.ObjectId;
        userID: Types.ObjectId;
        acceptableStart: Date;
        acceptableEnd: Date;
    }
}
export default Lottery;