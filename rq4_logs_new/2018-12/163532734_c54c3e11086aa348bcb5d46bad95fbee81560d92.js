class InfoToggle extends HTMLElement {
    constructor() {
        super()
        this.attachShadow({
            mode: 'open'
        })
        this.shadowRoot.innerHTML = /*html*/ `
        <style>
          #info-box {
             display: none;
        }
        </style>
        <button>Show</button>
        <p id="info-box">
        <slot></slot>
        </p>
        `
        this._toggleButton = this.shadowRoot.querySelector('button')
        this._infoBox = this.shadowRoot.querySelector('#info-box')
        this._isVisible = false
        this._toggleButton.addEventListener('click', this._toggleInfoBox.bind(this))
    }

    connectedCallback() {

    }

    _toggleInfoBox() {
      this._isVisible = !this._isVisible
      this._infoBox.style.display = this._isVisible ? 'block' : 'none'
      this._toggleButton.textContent = this._isVisible ? 'Hide' : 'Show'
    }
}

customElements.define('sw-info-toggle', InfoToggle)