const frameButtonTemplate = document.createElement('template');
frameButtonTemplate.innerHTML = `
    <style>
        @style('frame-button.scss')
    </style>
    <button>
        <slot></slot>
    </button>
`;

if ((<any>window).ShadyCSS) {
    (<any>window).ShadyCSS.prepareTemplate(frameButtonTemplate);
}

export class FrameButton extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({mode: 'open'});
        this.shadowRoot.appendChild(frameButtonTemplate.content.cloneNode(true))
    }
}

customElements.define('frame-button', FrameButton)