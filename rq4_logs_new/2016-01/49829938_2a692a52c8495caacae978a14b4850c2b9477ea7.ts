import { Platform } from 'ionic-framework/ionic';
import { Injectable } from 'angular2/core';
import { Observable } from 'rxjs/Observable';
import { Subscriber } from 'rxjs/Subscriber';
require('rxjs/add/operator/share');

declare let resolveLocalFileSystemURL: any;

@Injectable()
export default class SpiderStorageService {

    // TODO: isn't this a BehaviorSubject ??
    public spiders$: Observable<Array<Spider>>;
    private _spiders: Spider[] = [
        { name: 'MySpider', species: 'Scary', img: undefined },
        { name: 'MyOtherSpider', species: 'MoreScary', img: undefined }
    ];
    private _subscriber: Subscriber<Array<Spider>>;

    constructor(private platform: Platform) {
        this.spiders$ = new Observable<Array<Spider>>((subscriber: Subscriber<Array<Spider>>) => {
            this._subscriber = subscriber;
            return () => { console.log('SpiderStorageService observable disposed'); };
        }).share();
    }

    add(spider: Spider) {
        this._spiders.push(spider);
        this._subscriber.next(this._spiders);
        this.persist();
    }

    public read() {
        this.findFile()
            .then((entry: any) => new Promise((resolve, reject) => entry.file(resolve, reject)))
            .then((file: any) => new Promise((resolve, reject) => {
                let reader = new FileReader();
                reader.onload = resolve;
                reader.onerror = reject;
                reader.readAsText(file);
            }))
            .then((event: any) => { this._spiders = JSON.parse(event.target.result); this._subscriber.next(this._spiders); })
            // TODO: better error handling
            .catch((err: any) => { console.log('promise error', err); });
    }

    private persist() {
        this.findFile()
            .then((entry: any) => new Promise((resolve, reject) => entry.createWriter(resolve, reject)))
            .then((writer: any) => new Promise((resolve, reject) => {
                writer.onwrite = resolve;
                writer.onerror = reject;
                writer.write(JSON.stringify(this._spiders));
            }))
            // TODO: better error handling
            .catch((err: any) => { console.log('promise error', err); });
    }

    private findFile() {
        return this.platform.ready()
            .then(() => new Promise((resolve, reject) => { resolveLocalFileSystemURL(cordova.file.syncedDataDirectory, resolve, reject); }))
            .then((entry: any) => new Promise((resolve, reject) => entry.getFile('spiders.json', { create: true }, resolve, reject)));
    }

}

export class Spider {
    name: string;
    species: string;
    img: string;
}