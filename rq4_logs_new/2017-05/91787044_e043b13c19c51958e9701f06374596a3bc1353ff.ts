import {Component, OnInit, Input,} from '@angular/core';
import { suggestedWordsMock, wordsMock } from '../../shared/mock/words.mock';
import {Word} from "../../shared/models/word.model";

@Component({
  selector: 'wordsuggestion',
  templateUrl: './word-suggestion.component.html',
  styleUrls: ['./word-suggestion.component.css']
})
export class WordSuggestionComponent implements OnInit {
  words = suggestedWordsMock;
  modify: boolean;
  modified: Word[];
  selected: Word[];
  constructor() { }

  ngOnInit() {
    this.modify = false;
    this.modified = [] ;
    this.selected = [] ;
  }

  onModify(word)
  {
    //console.log(word);
    let v =this.modified.indexOf(word);
    //console.log(v);
    if(v == -1)
       this.modified.push(word);
    else
        this.modified = this.modified.filter(i => i !== word);
  }

  onDelete(word){
    //console.log(word);
    //this.words.slice(word,1);
    this.words = this.words.filter(item => item !== word);
    suggestedWordsMock.splice(suggestedWordsMock.indexOf(word),1);
    //console.log(this.words);
  }

  onSelected(word){

    //console.log(index);
    let v =this.selected.indexOf(word);
    //console.log(v);
    if(v == -1)
      this.selected.push(word);
    else
      this.selected = this.selected.filter(i => i !== word);
  }

  onSelectedList(){
    if(this.selected.length === 0)
      this.words.forEach(elem => this.selected.push(elem));
    else
      this.selected= [];
  }

  onDeleteAll()
  {
    this.words = this.words.filter(word => this.selected.indexOf(word) ==-1);
    this.words.forEach(word => suggestedWordsMock.splice(suggestedWordsMock.indexOf(word),1));
  }

  onModifyAll()
  {
    this.selected.forEach(word => this.modified.indexOf(word) === -1 ? this.modified.push(word) :
      this.modified.splice(this.modified.indexOf(word),1));
    this.selected.forEach(index => console.log(index));
    //console.log(this.modified);
    //console.log(this.selected);
  }

  onApprove(){
    //console.log(this.selected);
    //console.log(this.words);
    //this.words.forEach(we => console.log(we));
    this.words.forEach(word => {
      if(this.selected.indexOf(word) > -1 && wordsMock.indexOf(word) === -1)
      {
        //console.log((word.id-1)/2);
        wordsMock.push(word);
        this.words = this.words.filter(item => item !== word);
        //console.log("ABBA");
      }
      //console.log(this.selected.indexOf(word) > -1);
      //console.log(wordsMock.indexOf(word) == -1);
    });
    this.selected.forEach(word => suggestedWordsMock.splice(suggestedWordsMock.indexOf(word),1));
    this.selected = [];
  }

}