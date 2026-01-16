module dme.auctiondata {

    export class Auction_Query {
        realmName: string;
        auctions: Auction[];
        queryDate: Date;
    }

    export class Auction {
        public auc: Number;
        public item: Number;
        public owner: String;
        public ownerRealm: String;
        public bid: Number;
        public buyout: Number;
        public quantity: Number;
        public timeLeft: String;
        public rand: Number;
        public seed: Number;
        public context: Number;
    }
}