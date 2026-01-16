import React from "react";

const Button = ({onclick, children}) => {
    return <button
        type="button"
        onClick={onclick}
    >{children}
    </button>
};
export default Button;