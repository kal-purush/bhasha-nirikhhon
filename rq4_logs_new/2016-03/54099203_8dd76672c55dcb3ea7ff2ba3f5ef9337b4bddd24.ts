import {Injectable} from "angular2/core";
@Injectable()
export class ResourceService {
    public data = [];
    public key:String;
    public order = [];
    public getOrder = () => {
        return this.order[this.key];
    };
    public sortBy = (key) => {
        this.key = key;
        if (Object.keys(this.order).length === 0) {
            this.order[this.key] = 'asc';
        }
        if (this.order[this.key] === 'asc') {
            this.order = [];
            this.order[this.key] = 'desc';
            this.data.sort((a, b) => this.compare(a, b));
        } else {
            this.order = [];
            this.order[this.key] = 'asc';
            this.data.sort((a, b) => this.compare(b, a));
        }
    };

    public compare = (a, b) => {
        if (a[this.key] < b[this.key]) {
            return -1;
        } else if (a[this.key] > b[this.key]) {
            return 1;
        } else {
            return 0;
        }
    };
}