export default class CityWoeid{
    cid?: string = '';
    country?: string = ''
    name?: string = '';
    region?: string = '';
    woeid?: number = 0

    constructor(cityWoeid?: CityWoeid){
        this.cid = cityWoeid?.cid;
        this.country = cityWoeid?.country;
        this.name = cityWoeid?.name;
        this.region = cityWoeid?.region;
        this.woeid = cityWoeid?.woeid;
    }
}