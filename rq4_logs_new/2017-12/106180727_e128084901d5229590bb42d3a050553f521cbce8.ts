export enum RatingType{
    DELIVER = "DELIVER",
    CUSTOMER = "CUSTOMER"
}

export class Rating {
    constructor(
        private _id:number,
        private _rating:number,
        private _description:String,
        private _type:RatingType,
        private _ratedUser:String
    ) {}

    public get id():number{
        return this._id;
    }
    
    public get rating():number{
        return this._rating;
    }

    public get description():String{
        return this._description;
    }

    public get type():RatingType{
        return this._type;
    }

    public get ratedUser():String{
        return this._ratedUser;
    }
}