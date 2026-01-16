import {Article} from './article.model';
import {directory} from './directory'

export class Chapter implements directory{
    
    constructor(
        public id: number,
        public name: string,
        public articles: Article[],
        public titleID: number,
        public expanded: boolean = false,
        public reviewed: boolean = false,
        public type: string = 'chapter'
    ){}
    
    static fromJson(json: string){
        var data = JSON.parse(json);
        return new Chapter(
            data.id, data.name,data.articles,data.titleid
        )
    }
}