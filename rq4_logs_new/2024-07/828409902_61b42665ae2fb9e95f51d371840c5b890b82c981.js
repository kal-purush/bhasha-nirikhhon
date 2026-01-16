export class AppBar extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <style>
        .app-bar {
          background-color: #050C9C; /* Dark Blue */
          color: white;
          padding: 1rem;
          text-align: center;
          font-size: 1.5rem;
          box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
          margin-bottom: 1rem;
        }
      </style>
      <div class="app-bar">Notes App</div>
    `;
  }
}

if (!customElements.get('app-bar')) {
  customElements.define('app-bar', AppBar);
}

export default AppBar;