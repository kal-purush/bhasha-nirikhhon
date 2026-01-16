export default class Aboutus extends HTMLElement {
    constructor() {
      super();
    }
  
    connectedCallback() {
      const shadowRoot = this.attachShadow({mode:'open'})
      shadowRoot.innerHTML = `
      <style>
        :host{
          display: block;
        }
          .wrap {
              width: 100vw;
              height: 100vh;
              background-color: white;
              overflow: hidden;
          }
      </style>
      <div class='wrap'>
         <div>
            sobre nosotros
         </div>
      </div>
      `;
    }
  }
  
  window.customElements.define("fabs-about", Aboutus);