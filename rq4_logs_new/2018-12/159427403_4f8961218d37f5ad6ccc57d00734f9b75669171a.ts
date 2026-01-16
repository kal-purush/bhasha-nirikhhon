import { PolymerElement, html } from "@polymer/polymer";

export class YTJBThumbnail extends PolymerElement {

    private _id: string;
    
    private _title: string;
    private _author: string;

    private _url: string;
    private _width: number;
    private _height: number;

    private _date: Date;

    constructor(data:any) {
        super();

        this._id = data.id;
        
        this._title = data.title;
        this._author = data.author;

        this._url = data.thumbnail.url;
        this._width = data.thumbnail.width;
        this._height = data.thumbnail.height;

        this._date = new Date(data.date);

    }

    connectedCallback() {
        super.connectedCallback();

        this.style.width = this._width + "px";
    }

    static get properties() {
        return {

            _id: String,

            _title: String,
            _author: String,

            _url: String,
            _width: Number,
            _height: Number,

            _date: Object

        }
    }

    static get template() {
        return html`
            <style>
                :host {
                    display: block;
                    padding: 1em;
                } 
            </style>

            <img src="[[_url]]" />
            <h2>[[_title]]</h2>
            <p>[[_author]]</p>
        `;
    }

}

customElements.define('ytjb-thumbnail', YTJBThumbnail);