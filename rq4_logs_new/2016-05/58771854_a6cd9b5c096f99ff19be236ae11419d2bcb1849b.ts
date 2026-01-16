/**
 * Created by igor on 1/13/16.
 */
// https://developer.mozilla.org/en-US/docs/Web/API/GlobalFetch/fetch
// uri: 'https://api.flickr.com/services/rest/?',
// apiKey: '7fbc4d0fd04492d32fa9a2f718c6293e'
// queryMethod: 'flickr.photos.search',
// ${this.uri}method=${this.qyeryMethod}&api_key=${this.apiKey}&text=${text}&page=1&format=json&nojsoncallback=1`

interface Underscore {
    debounce<T extends Function>(fn: T, wait: number, immediate?: boolean): T;
}

declare var _: Underscore;

declare var Promise: {
    prototype: PromiseLike<any>;
    all(promises: any[]): PromiseLike<any>
};

interface FormData {
    append(name: any, value: any, blobName?: string): void;
    get(name: any): any;
}

declare var FormData: {
    prototype: FormData;
    new (form?: HTMLFormElement): FormData;
};

interface Headers {
    append: (name: string, value: string) => void;
    delete: (name: string) => void;
    values: any[];
}

interface InitRequest {
    method: string;
    headers: string|Headers;
    cache: string;
}

interface Request {
    method: string;
    redirect: string;
    cache: string;
}

declare var Request: {
    prototype: Request;
    new (input: string|Request, init?: InitRequest): Request;
};

interface InitResponse {
    status: string;
    statusText: string;
    headers: string|Headers;
}

interface ResponseBody {
    FormData: FormData;
    BufferSource: any;
}

interface Response extends ResponseBody {
    arrayBuffer: () => PromiseLike<ArrayBuffer>;
    blob: () => PromiseLike<Blob>;
    json(): PromiseLike<any>;
}

declare var Response: {
    prototype: Response;
    new(input: ResponseBody, init: InitResponse): Response;
};

declare function fetch(input: string|Request): PromiseLike<Response>

type opt = {
    uri: string,
    queryMethod: string,
    apiKey: string
}

interface IPhoto {
    farm: number;
    id: string;
    owner: string;
    server: string;
    secret: string;
    title: string;
}

interface IUser {
    person: {
        id: string;
        username: {
            _content: string
        }
    };
}

abstract class Component {
    private element: HTMLElement;

    protected abstract createDom(): void;

    protected abstract enterDocument(): void;

    protected setElement(element: HTMLElement, cls?: string, html?: string): void {
        this.element = element;

        if (cls) {
            this.element.className = cls;
        }

        if (html) {
            this.element.innerHTML = html;
        }
    }

    protected getElementBySelector(selector: string): Element {
        return this.element.querySelector(selector);
    };

    public getElement(): HTMLElement {
        return this.element;
    };

    public render(parent?: HTMLElement): void {
        let parentElement: HTMLElement = parent || document.body;
        this.createDom();
        parentElement.appendChild(this.element);
        this.enterDocument();
    };
}

class App extends Component {
    private uri: string;
    private queryMethod: string;
    private apiKey: string;
    private gallerySearch: HTMLFormElement;
    private galleryList: HTMLDivElement;
    private photos: IPhoto[];

    constructor({uri, queryMethod, apiKey}) {
        super();
        this.uri = uri;
        this.queryMethod = queryMethod;
        this.apiKey = apiKey;
    }

    protected createDom(): void {
        this.setElement(
            document.createElement('section'),
            'gallery',
            `<form class="gallery-search" action="javascript:;">
        <input name="text" class="gallery-search-input" type="text" placeholder="Type your request" autocomplete="off" />
        <button class="gallery-search-button">Search</button>
    </form>
    <div class="gallery-list"></div>`
        );
    }

    protected enterDocument(): void {
        this.gallerySearch = <HTMLFormElement>this.getElementBySelector('.gallery-search');
        this.galleryList = <HTMLDivElement>this.getElementBySelector('.gallery-list');
        this.gallerySearch.addEventListener('submit', _.debounce(
            this.search.bind(this, this.updateList.bind(this)), 500
            )
        );
    };

    private updateList(body: any): void {
        let content = ``,
            authors: PromiseLike<any>[] = [];

        this.photos = <IPhoto[]>body.photos.photo;
        this.photos.sort((a, b) => a.title.localeCompare(b.title));
        for (let photo of this.photos) {
            content += `<article class="gallery-item gallery-owner-id-${photo.owner.replace('@', '-')}">
<img src="https://farm${photo.farm}.staticflickr.com/${photo.server}/${photo.id}_${photo.secret}.jpg" class="gallery-item-picture" />
<p class="gallery-item-description">${photo.title}</p>
</article>`;
            authors.push(fetch(
                `${this.uri}method=flickr.people.getInfo&api_key=${this.apiKey}&user_id=${photo.owner}&format=json&nojsoncallback=1`
            ));
        }
        this.galleryList.innerHTML = content;

        Promise.all(authors).then(this.addAuthors);
    }

    private addAuthors = (results: Response[]) => { // получился какой-то кошмар )
        let authors: string[] = [];
        results.forEach(response => {
            response.json().then((result: IUser): void => {
                let id: string = result.person.id,
                    username: string = result.person.username._content;

                if (authors.indexOf(id) === -1) {
                    let items = this.galleryList.querySelectorAll('.gallery-owner-id-' + id.replace('@', '-')),
                        itemsLen = items.length;

                    for (let i = 0; i < itemsLen; i++) {
                        let div = document.createElement('div');
                        div.className = 'gallery-item-author';
                        div.innerHTML = username;

                        items[i].appendChild(div);
                    }
                    authors.push(id);
                }
            });
        });
    };

    private search(cb: (body: any) => any): void {
        let formData: FormData = new FormData(this.gallerySearch),
            text = formData.get('text');

        if (!text) {
            return;
        }

        let url = new Request(
            `${this.uri}method=${this.queryMethod}&api_key=${this.apiKey}&text=${text}&page=1&format=json&nojsoncallback=1`
        );
        this.getPhotos(url, cb);
    }

    private getPhotos(input: string|Request, cb: (body: any) => any): void {
        fetch(input)
            .then((response: Response): PromiseLike<any> => {
                return response.json();
            }).then(cb);
    }
}


let flickr = new App({
    uri: 'https://api.flickr.com/services/rest/?',
    queryMethod: 'flickr.photos.search',
    apiKey: '7fbc4d0fd04492d32fa9a2f718c6293e'
});
flickr.render();