export class AuctionObject{

    constructor(public id: number,
                public image: string,
                public title: string,
                public date: Date,
                public expectedEnd: Date,
                public category : string,
                public description:string,
                public timeout: any,
                public initialBid: any,
                public owner : string){
                }
            
    static createEmpty() : AuctionObject
    {
        return new AuctionObject(0,"","",new Date,new Date,"","","","","");
    }

    static createFromJson(json : any) : AuctionObject
    {
        let date = new Date(json["date"])
        let end = new Date(json["expectedEnd"])
        return new AuctionObject(json["id"], json["image"], json["title"], date, end, json["category"], 
        json["description"], json["timeout"], json["initialBid"], json["owner"]);
    }
    
    resetDate(time : Date)
    {
        let result = new Date(time)
        this.date = result;
    }

    resetExpectedEnd(time : Date):void
    {
        let newEnd = new Date (time);
        newEnd.setSeconds(newEnd.getSeconds() + this.timeout)
        this.expectedEnd = newEnd
    }

    incrementExpectedEnd()
    {
        let incrementedSeconds = this.expectedEnd.getSeconds() + this.timeout;
        this.expectedEnd.setSeconds(incrementedSeconds);
    }

    isStarted(time : Date):boolean
    {
        let auctionStart = this.date;
        return auctionStart <= time; 
    }

    isNotOver(time : Date):boolean
    {
        let auctionEnd = this.expectedEnd.getTime();
        let timeout = this.timeout * 1000;

        return time.getTime() + timeout > auctionEnd;
    }

    isContinuing(time : Date):boolean
    {
        return this.isStarted(time) && this.isNotOver(time);
    }

    getRemainingTime(time : Date) : number
    {
        let resultMiliseconds = this.expectedEnd.getTime() - time.getTime();
        return Math.trunc(resultMiliseconds/1000);
    }

}