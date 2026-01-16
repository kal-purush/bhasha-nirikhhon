import {Component, OnInit, Input, Output, EventEmitter,} from '@angular/core';
import { suggestedWordsMock, wordsMock } from '../mock/words.mock';
import {Word} from "../models/word.model";

@Component({
  selector: 'wordstable',
  templateUrl: 'words-table.component.html',
  styleUrls: ['words-table.component.css']
})
export class WordsTableComponent implements OnInit {
  @Input() selectionColumn: boolean = false;
  @Input() range: boolean =false;
  @Input() name: boolean = false;
  @Input() meaning: boolean = false;
  @Input() wordClass: boolean = false;
  @Input() modifyColumn: boolean = false;
  @Input() deleteColumn: boolean = false;
  @Input() approve: boolean = false;
  @Input() words: Array<Word>;
  @Output() approved = new EventEmitter<any>();
  @Output() Deleted = new EventEmitter<any>();
  //words = suggestedWordsMock;
  modified: Word[];
  selected: Word[];

  ngOnInit() {
    this.modified = [] ;
    this.selected = [] ;
    //this.words = new Array();
    console.log(this.words.toString()+ "ABBA");
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

    this.words = this.words.filter(item => item !== word);
    let array =[word];
    console.log(word);
    this.Deleted.emit({array});
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

  onSelectedList(event){
    console.log(event);
    if(event.srcElement.checked === true )
      this.words.forEach(elem => this.selected.push(elem));
    else
      this.selected= [];
  }

  onDeleteAll()
  {
    let array =[];
    this.words.forEach(word => {
      if(this.selected.indexOf(word) > -1)
          array.push(word);
    });
    this.words = this.words.filter(word => this.selected.indexOf(word) ==-1);
    this.Deleted.emit({array});
  }

  onModifyAll()
  {
    this.selected.forEach(word => this.modified.indexOf(word) === -1 ? this.modified.push(word) :
      this.modified.splice(this.modified.indexOf(word),1));
    //this.selected.forEach(index => console.log(index));

    console.log(this.words + " ABBA");
    //console.log(this.modifided);
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