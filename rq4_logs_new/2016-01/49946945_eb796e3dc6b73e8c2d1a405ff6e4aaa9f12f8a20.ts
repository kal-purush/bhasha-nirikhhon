let exec = require("promised-exec");
import * as Promise from "bluebird";


function prepare_address(addresses: IAddress[]) {
    let readdr = [];
    for (var i = 0; i < addresses.length; i++) {
        readdr[i] = { uuid: addresses[i].uuid, dev: addresses[i].dev, address: addresses[i].address };

    }
    return JSON.stringify(readdr);
}

interface IAPI{
//todo
}
interface IAddress{
    uuid:string;
    dev:string;
    address:number;
}
class AJS {
    addresses: IAddress[];
    timezone: string;
    constructor(addresses: IAddress[], timezone: string) {
        this.addresses = addresses;
        this.timezone = timezone;
    }
    data() {
        let addresses = prepare_address(this.addresses);
        let timezone = this.timezone;
        return new Promise<IAPI[]>(function(resolve, reject) {
            exec(__dirname + "/aurora.sh -a \"" + addresses + "\" -t \"" + timezone + "\"").then(function(data:string) {
                resolve(JSON.parse(data));
            }).catch(function(err) {
                reject(err);
            });
        });
    }
}
export = AJS