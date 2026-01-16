/**
 * Author: Richard R.
 * Email: rrivas@usaly.co
 *
 * Main component to encapsulate components
 */
import constants from "../../utils/constants";
import { getAuth, signInWithEmailAndPassword } from "firebase/auth";

class LoginComponent {
    stringElements = {
        idForm: '#form-id',
        txtUser: '#txt-user',
        txtPwd: '#txt-pwd',
        btnSubmit: '#btn-request',
    };
    domContainer = document.querySelector(constants.sections.idContainer);
    domForm = null;
    auth = getAuth();
    isRequest = false;

    template () {
        return `
            <h1>
                Inicio de sesión
            </h1>
            <div class=form">
                <form id="${this.stringElements.idForm.replace('#', '')}">
                    <input type="text" id="${this.stringElements.txtUser.replace('#', '')}" placeholder="Correo">
                    <input type="password" id="${this.stringElements.txtPwd.replace('#', '')}" placeholder="Contraseña">
                    <button type="submit" id="${this.stringElements.btnSubmit.replace('#', '')}">Iniciar sesión</button>
                </form>
            </div>
        `;
    }

    events() {
        this.domForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const email = document.querySelector(this.stringElements.txtUser).value;
            const pwd = document.querySelector(this.stringElements.txtPwd).value;
            this.login(email, pwd);
        });
    }

    // Attach HTML components to parent div
    registerElement() {
        this.domContainer.insertAdjacentHTML('beforeend', this.template());
        this.domForm = document.querySelector(this.stringElements.idForm);
        this.events();
    }

    login(email, pwd) {
        if (this.isRequest) {
            return;
        }
        const btnRequest = document.querySelector(this.stringElements.btnSubmit);
        btnRequest.setAttribute('disabled', 'disabled');
        btnRequest.innerHTML = 'Iniciando...';
        this.isRequest = true;
        signInWithEmailAndPassword(this.auth, email, pwd)
            .then((userCredential) => {
                console.log('Login successfully');
            })
            .catch((error) => {
                const errorCode = error.code;
                const errorMessage = error.message;
                console.log('auth', errorCode, errorMessage);
                btnRequest.removeAttribute('disabled');
                btnRequest.innerHTML = 'Iniciar sesión';
                alert(errorMessage);
            });
    }

    render() {
        // Add DOM HTML component
        this.registerElement();
    }
}

export default LoginComponent;