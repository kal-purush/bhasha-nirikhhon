import { Component, OnInit } from '@angular/core';
import {CollectionsService} from '../../word-collection/Collection.service';

@Component({
  selector: 'hangman',
  templateUrl: 'hangman.component.html',
  styleUrls: ['hangman.component.css']
})
export class HangmanComponent implements OnInit {
  AlphaBet = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'];
  imgURL: string;
  Letters: Array<string>  = new Array();
  UsedLetters:  Array<string> = new Array();
  UsedIDs: Array<number> = new Array();
  AllIDS: Array<number> = new Array();
  word: string;
  Faults=0;
  imUrlBase = 'src/app/words/practice/hangman/src/';
  message = "";
  constructor(private collectionsService: CollectionsService) { }

  ngOnInit() {
    this.collectionsService.getCollections();
    this.imgURL = 'src/app/words/practice/hangman/src/10.png';
  }

  OnChanged(id){
    this.SetDefeault();
    this.UsedIDs.splice(0, this.UsedIDs.length);
    this.AllIDS = this.collectionsService.getIDs(id);
    this.imgURL = 'src/app/words/practice/hangman/src/0.png';
    this.word = this.collectionsService.getWord(this.GetRandId());
    console.log(this.word);
    for(let i = 0;i<this.word.length; i++)
        this.Letters.push('_');

  }

  GetRandId(){
    let id;
    if(this.UsedIDs.length == this.AllIDS.length)
      return -1;
    do{
      id = Math.floor(Math.random() * this.AllIDS.length);
    } while( this.UsedIDs.includes(id) );
    this.UsedIDs.push(id);
    return this.AllIDS[id];

  }

  OnSelect(letter) {
    if (this.word !== undefined) {
      if(this.UsedLetters.indexOf(letter) > -1)
        return;
      this.UsedLetters.push(letter);
      let index, startIndex =0;
      while( (index = this.word.indexOf(letter.toLowerCase(),startIndex)) > -1) {
        startIndex = index + 1;
        this.Letters[index] = letter;
      }
      if(this.Letters.indexOf(letter) == -1)
      {
          if(++this.Faults >= 10)
            this.imgURL = this.imUrlBase.concat('lose','.png');
          else
            this.imgURL = this.imUrlBase.concat((this.Faults).toString(),'.png');
      }
      else{
          if(this.Letters.indexOf('_') == -1)
            this.imgURL = this.imUrlBase.concat('win','.png');
      }
    }
  }

  OnNextWord(){
      this.SetDefeault();
      this.word = this.collectionsService.getWord(this.GetRandId());
      if(this.word == undefined) {
        this.message = "Nincs több szó";
        return;
      }
          for(let i = 0;i<this.word.length; i++)
        this.Letters.push('_');
      console.log(this.word);
  }

  SetDefeault(){
    this.message="";
    this.Letters.splice(0,this.Letters.length);
    this.UsedLetters.splice(0, this.UsedLetters.length);
    this.Faults = 0;
    this.imgURL = 'src/app/words/practice/hangman/src/0.png';
    this.word = undefined;
  }

  onAgainCollection(){
    this.SetDefeault();
    this.UsedIDs.splice(0, this.UsedIDs.length);
    this.imgURL = 'src/app/words/practice/hangman/src/0.png';
    this.OnNextWord();
  }
}