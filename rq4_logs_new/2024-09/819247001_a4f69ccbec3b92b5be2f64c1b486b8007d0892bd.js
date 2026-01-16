import { LitElement,html } from "lit";

class MyReactive extends LitElement{
 static properties = {
    name: {
        type:String
    },
 };

 constructor(){
    super();
    this.name = 'Belajar reactive property di LIT';
 }

 render() {
    return html` <h1>${this.name}</h1>`;
 }

}

customElements.define('reactive-element', MyReactive);