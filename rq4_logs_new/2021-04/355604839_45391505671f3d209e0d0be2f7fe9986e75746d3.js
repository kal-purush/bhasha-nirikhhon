import React, { Component } from "react";

import Button from "/UI/Button/Button"


class Logout extends Component {

    loginOutHandler = () => {
        // Modification du stockage de session lors de la déconnexion
        if(sessionStorage.getItem("isUserLogged")) {
            sessionStorage.removeItem("token");
            sessionStorage.removeItem("isUserLogged")

            this.props.loginOut(false, "");
        }
    }

    render() {
        return (
            <div>
                <Button btnType="Danger" clicked={this.loginOutHandler}>
                    Déconnexion
                </Button>
            </div>
        );
    }
}

export default Logout;