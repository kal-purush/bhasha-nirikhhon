import {
    LitElement,
    html,
    css,
    classMap,
  } from “https://cdn.jsdelivr.net/gh/lit/dist@2/core/lit-core.min.js”;
  class Component extends LitElement {
    static properties = {
        count: {type: Number}
    label: {type: String}

    }

    };

    static styles = css`
    span{
    colour:red;}

span.error{colour: red;}
span.great{colour: green;}

    `

    add(){
        this.count = this.count +1;

    }

    remove(){
        this.count = this.count - 1;
    }

const valueClass = classMap({
error:
    great:

})

    render() {

      return html`
        <div>
            <button @click="${this.remove}">-</button>
        ${this.label}: ${this.count}
        <button @click="${this.add}">+</button>
    </div>
        `;
    }
  


customElements.define("value-counter", Component);