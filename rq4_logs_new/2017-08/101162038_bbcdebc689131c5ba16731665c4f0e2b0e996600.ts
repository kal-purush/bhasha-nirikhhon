import { Injectable } from '@angular/core';

@Injectable()
export class WidgetLibraryService {
  widgets: IWidget[];

  constructor() { 
    //init the array
    this.widgets = [];
    //Register all widgets here
    this.widgets.push(new TestWidget());
    this.widgets.push(new Test2Widget());
    this.widgets.push(new Test3Widget());
    //--------> Add more here

    //Sort array based on title name
    this.widgets.sort(function (a, b){
      var nameA = a.title.toUpperCase();
      var nameB = b.title.toUpperCase();
      if(nameA < nameB){
        return -1;
      }
      if(nameA > nameB){
        return 1;
      }
      //same names
      return 0;
    });
  }

  //Life cycle hook
  ngOnInit(){}

}

//Interface for all widgets to implement
interface IWidget{
  title: string;
  spawn():string;
}

//Implementations ---------------->
class TestWidget implements IWidget{
  title:string;

  constructor(){
    this.title = "Test Boks 1";
  }
  spawn():string{
    return "<p> hello world From 1 </p>";
  }
}

class Test2Widget implements IWidget{
  title:string;

  constructor(){
    this.title = "Test Boks 2";
  }
  spawn():string{
    return "<p> hello world 2</p>";
  }
}

class Test3Widget implements IWidget{
  title:string;

  constructor(){
    this.title = "Test Boks 3";
  }
  spawn():string{
    return "<p> hello world 3</p>";
  }
}