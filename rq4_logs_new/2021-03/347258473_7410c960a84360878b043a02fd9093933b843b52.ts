export class Item {
    public imagePath: string;
    public category: string;
    public keywords: string;
    public title: string;
    public countryCreator: string;
    public year: number;
    public condition: string;
    public media: string;
    public description: string;
    public references: string;

    constructor(imagePath: string, category: string, keywords: string=null, title: string ){
        this.imagePath = imagePath;
        this.category = category;
        this.keywords = keywords;
        this.title = title;
    }
}