/// <reference path="Paper.ts"/>

module Sanara.Core {
  export interface Fragment {
      paint(paper: Paper) : void;
      paintMe(paper : Paper) : void;
  }
  export class BaseFragment implements Fragment {
      children : [Fragment];
      constructor (children : [Fragment] = <[Fragment]>[]) {
          this.children = children;
      }
      paint(paper: Paper) {
          paper.context.save();
          this.paintMe(paper);
          paper.context.restore();
      }
      paintMe(paper : Paper) {
         this.children.forEach(function (v) {
             v.paint(paper);
         })
      }
  }
}