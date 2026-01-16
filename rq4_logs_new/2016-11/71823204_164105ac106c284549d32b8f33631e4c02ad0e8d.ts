// https://developer.mozilla.org/en-US/docs/Web/API/GlobalFetch/fetch
// https://github.com/iliakan/ts-course/blob/master/ts-lesson-3/demo-flikr-app/scripts/fetch.ts

import * as _ from 'lodash';

declare const Q:{
    all:(arr:PromiseLike<any>[]) => PromiseLike<any>
};

export class FlikrAppAsModuleHomework {
    public getPhotos:(input:string | Request, cb:(body:any) => any) => void;
    protected searchHandler:(url:Request) => PromiseLike<(body:any) => any>;
    protected elem:HTMLElement;
    protected input:HTMLInputElement;
    protected searchButton:HTMLButtonElement;
    protected imagesBox:HTMLDivElement;
    protected uri:string;
    protected qyeryMethod:string;
    protected apiKey:string;
    protected photos:IPhoto[];

    constructor({elem, uri, queryMethod, apiKey}:opt) {
        this.elem = elem;
        this.uri = uri;
        this.qyeryMethod = queryMethod;
        this.apiKey = apiKey;
        this.input = <HTMLInputElement>this.elem.querySelector('.flickr-search-input');
        this.imagesBox = <HTMLDivElement>this.elem.querySelector('.image-area');
        this.searchButton = <HTMLButtonElement>this.elem.querySelector('.flickr-search-button');
        let debounceClick = _.debounce(this.search.bind(this, this.render.bind(this)), 500);
        this.searchButton.addEventListener('click', debounceClick);
    }

    protected render(body:any):void {
        this.photos = _.sortBy<IPhoto>(body.photos.photo, 'title');
        Q.all(_.map(this.photos, (photo:IPhoto) => {
                let url = new Request(`${this.uri}method=flickr.people.getInfo&
            api_key=${this.apiKey}&user_id=${photo.owner}&format=json&nojsoncallback=1`);
                return this.searchHandler(url);
            }))
            .then((users:any):void => {
                let content = ``;
                this.photos = _.map(this.photos, (photo:IPhoto, i:number):IPhoto => {
                    photo.owner = users[i].person.username._content;
                    return photo;
                });
                for (let photo of this.photos) {
                    content += `<div  class='image-box'>
                        <img src='https://farm${photo.farm}.staticflickr.com/${photo.server}/${photo.id}_${photo.secret}.jpg' />
                        <p><span>${photo.owner}: </span>${photo.title}</p>
                        </div>`;
                }
                this.imagesBox.innerHTML = content;
            });
    }

    protected search(cb:(body:any) => any):void {
        let text = this.input.value;
        let url = new Request(`${this.uri}method=${this.qyeryMethod}&
        api_key=${this.apiKey}&text=${text}&page=1&format=json&nojsoncallback=1`);
        this.searchHandler(url).then(cb);
    }

    public set setSearchHandler(handler:(input:string | Request) => PromiseLike<any>) {
        this.searchHandler = handler;
    }
}