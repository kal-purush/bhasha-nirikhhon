export default class Category {
    constructor(
        public _id: string,
        public name: string,
        public imageSrc: string,
    ) {
        this._id = _id;
        this.name = name;
        this.imageSrc = imageSrc;
    }
}