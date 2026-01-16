// https://developer.mozilla.org/en-US/docs/Web/API/GlobalFetch/fetch

//`${this.uri}method=${this.qyeryMethod}&api_key=${this.apiKey}&text=${text}&page=1&format=json&nojsoncallback=1`
//https://farm${photo.farm}.staticflickr.com/${photo.server}/${photo.id}_${photo.secret}.jpg
// uri: 'https://api.flickr.com/services/rest/?',
//     queryMethod: 'flickr.photos.search',
//     apiKey: '7fbc4d0fd04492d32 fa9a2f718c6293e'


// Promise -> Q , lodash
interface RequestInit {
    method: string;
    url: string;
    mode: string;
}

interface Request {
    method: string;
    url: string;
    context: string;
}

declare const Request: {
    prototype: Request;
    new(body: string|Request, init?: RequestInit): Request;
}

interface ResponseInit {
    status: string;
    statusText: String;
}

interface ResponseBody {
    blob: Blob;
    formData: FormData;
}

interface Response {
    blob: ()=>PromiseLike<Blob>;
    formData: ()=>PromiseLike<FormData>;
    json: <T>()=>PromiseLike<T>;
}

declare const Response: {
    prototype: Response;
    new(body: ResponseBody, init: ResponseInit): Response;
};

declare function fetch(input: string|Request): PromiseLike<Response>

type opt = {
    elem: HTMLDivElement;
    uri: string;
    queryMethod: string;
    apiKey: string
}

interface flickrPhoto {
    farm: number,
    id: string,
    isfamily: number,
    isfriend: number,
    owner: string,
    secret: string,
    server: string,
    title: string
}

interface flickrPhotosSearchResponce {
    photos: {
        photo: flickrPhoto[]
    }
}

class FlickrApp {
    protected elem: HTMLDivElement;
    protected input: HTMLInputElement;
    protected searchButton: HTMLButtonElement;
    protected imagesBox: HTMLDivElement;
    protected uri: string;
    protected queryMethod: string;
    protected apiKey: string;
    protected photos: flickrPhoto[];

    public constructor(opt: opt) {
        this.elem = opt.elem;
        this.uri = opt.uri;
        this.queryMethod = opt.queryMethod;
        this.apiKey = opt.apiKey;
        this.input = <HTMLInputElement>document.querySelector('.flickr-search-input');
        this.imagesBox = <HTMLDivElement>document.querySelector('.image-area');
        this.searchButton = <HTMLButtonElement>document.querySelector('.flickr-search-button');
        this.searchButton.addEventListener('click', _.debounce(this.search.bind(this, this.render.bind(this)), 100))
    }

    protected render(body: flickrPhotosSearchResponce): void {
        this.photos = body.photos.photo;
        let content = ``;

        this.photos.sort((a: flickrPhoto, b: flickrPhoto) => a.title > b.title ? 1 : -1);

        for (let photo of this.photos) {
            content += `<div class="image-box">
                        <img src="https://farm${photo.farm}.staticflickr.com/${photo.server}/${photo.id}_${photo.secret}.jpg" />
                        <p>${photo.title}</p>
                        </div>`
        }
        this.imagesBox.innerHTML = content;

    }

    protected search(cb: (body: flickrPhotosSearchResponce) => void): void {
        if (!this.input.value) {
            return;
        }
        let text = this.input.value;
        this.input.value = '';
        let url = new Request(`${this.uri}method=${this.queryMethod}&api_key=${this.apiKey}&text=${text}&page=1&format=json&nojsoncallback=1`);
        this.getPhotos(url, cb)

    }

    protected getPhotos(input: string|Request, cb: (body: flickrPhotosSearchResponce) => void): void {
        fetch(input)
            .then((response: Response): PromiseLike<flickrPhotosSearchResponce> => response.json())
            .then(cb)
    }
}


let elem = <HTMLDivElement>document.querySelector('.flickr-box');
let flickr = new FlickrApp({
    elem,
    uri: 'https://api.flickr.com/services/rest/?',
    queryMethod: 'flickr.photos.search',
    apiKey: '7fbc4d0fd04492d32fa9a2f718c6293e'
});