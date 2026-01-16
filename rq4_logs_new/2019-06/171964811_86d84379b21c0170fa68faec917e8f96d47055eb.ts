import { Types } from 'mongoose';
declare namespace Subscription {
    interface Model {
        _id?: Types.ObjectId;
        clubID?: Types.ObjectId;
        userID?: Types.ObjectId;
        productID?: Types.ObjectId;
        name?: string;
        description?: string;
        active?: boolean;
        cancelAt?: Date;
        cancelledAt?: Date;
        currentPeriodStart?: Date;
        cancelAtPeriodEnd?: boolean;
        interval?: Interval;
        intervalCount?: number;
    }
    enum Interval {
        Day = "Day",
        Week = "Week",
        Monthly = "Monthly",
        Yearly = "Yearly"
    }
}
export default Subscription;