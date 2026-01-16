// /// THIS CONTROLLS ALL THE ALERTS TO BE SHOWN /////

// type: 'success' or 'error' 

export const hideAlert = () => {
    const el = document.querySelector('.alert');

    // JS Hack to move to parent & remove the HTML of child elem {here its el}
    if (el) el.parentElement.removeChild(el);
}

export const showAlert = (type, msg) => {
    // 1. Always hide alerts first
    hideAlert();

    // 2. Create the alert's html
    const markup = `<div class="alert alert--${type}">${msg}</div>`;
    
    // 3. Inject html where needed
    document.querySelector('body').insertAdjacentHTML('afterbegin', markup);

    // 4. Always hide un necessary alerts after 5s {if the user doesn't hide them }
    window.setTimeout(hideAlert, 5000);
}