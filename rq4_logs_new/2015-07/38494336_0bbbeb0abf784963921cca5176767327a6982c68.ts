/// <reference path="../../library.d.ts" />
import {VocabularyGenerator} from '../../common/pedagogicLogic/WordsGenerator';

/**
 * Created by yotam on 6/7/2015.
 */
export class VocabularyEditorService {
    static $inject = [];
    private words : any[] = [];
    private myDataRef : any;
    private baseCollectionUrl : string = 'https://amber-fire-4238.firebaseio.com/dictionary';

    constructor(private $http:any) {
        this.myDataRef = new Firebase(this.baseCollectionUrl);
        var self =this;
        $http.get(this.baseCollectionUrl +'.json').success(function (data) {
            console.log("data is loaded");
            for (var key in data) {
                var item = data[key];
                item.key = key;
                self.words.push(item);
            }
        });
    }

    getWords() {
        return this.words;
    }
    getWordsObjects() {
        console.log("getting vocabulary objects.. ");

        var wordObj;
        var result = [];
        for (let word of this.words){
            if (word.symbols){
                wordObj = VocabularyGenerator.createWordFromString(word.symbols, word.imagePath);
                result.push(wordObj);
            }

        }
        return result;
    }
    setWord(word) {

        var wordRef = new Firebase(this.baseCollectionUrl +"/"+ word.key);

        wordRef.update({symbols: word.symbols});
    }


}
