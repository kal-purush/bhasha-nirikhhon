import ISpotifyLoginRes from "../../interfaces/spotfiy/ISpotifyLoginRes";

export default class SpotifyLoginRes implements ISpotifyLoginRes {

    private _id: string;
    private _email: string;
    private _birthdate: string;
    private _display_name: string;
    private _country: string;
    private _image_url: string;

    constructor(data: any) {
        this._id = data.id;
        this._email = data.email;
        this._birthdate = data.birthdate;
        this._display_name = data.display_name;
        this._country = data.country;
        this._image_url = data.image_url;
    }

    get id(): string {
        return this._id;
    }

    set id(value: string) {
        this._id = value;
    }

    get email(): string {
        return this._email;
    }

    set email(value: string) {
        this._email = value;
    }

    get birthdate(): string {
        return this._birthdate;
    }

    set birthdate(value: string) {
        this._birthdate = value;
    }

    get display_name(): string {
        return this._display_name;
    }

    set display_name(value: string) {
        this._display_name = value;
    }

    get country(): string {
        return this._country;
    }

    set country(value: string) {
        this._country = value;
    }

    get image_url(): string {
        return this._image_url;
    }

    set image_url(value: string) {
        this._image_url = value;
    }

}