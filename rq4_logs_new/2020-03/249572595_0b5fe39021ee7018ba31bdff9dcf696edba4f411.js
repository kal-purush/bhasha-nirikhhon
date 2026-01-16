import { LitElement, html } from 'lit-element';

class GoogleSignIn extends LitElement {
    static get properties() {
        return {
            logoutUrl: { type: String },
            csrfParameterName: { type: String },
            csrfToken: { type: String },
            loggedIn: { type: Boolean }
        }
    }
    constructor() {
        super();
    }
    render() {
        return html`
        <div ?hidden=${!this.loggedIn}>
            <form method="post" action="${this.logoutUrl}">
            <input type="hidden" name="${this.csrfParameterName}" value="${this.csrfToken}"/>
                <vaadin-button @click="${e => this.submit(e)}">Logout</vaadin-button>
            </form>
        </div>
        <slot></slot>
        `;
    }
    submit(event) {
        this.shadowRoot.querySelector('form').submit();
    }
}
customElements.define('google-sign-in', GoogleSignIn);