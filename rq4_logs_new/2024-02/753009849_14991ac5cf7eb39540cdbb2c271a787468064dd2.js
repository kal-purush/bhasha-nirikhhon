if (typeof scriptUrl === 'undefined') {
    const scriptUrl = 'https://script.google.com/macros/s/AKfycbz8jamsRFSV2fxn4z2IRIS4eEJ9vf6eWg2ptoCphRUv8OSxhJkUSx0m0LVDvlbpqNWJyg/exec';
    const forms = document.forms['PortFolio_Data'];

    forms.addEventListener('submit', e => {
        e.preventDefault();
        fetch(scriptUrl, { method: 'POST', body: new FormData(forms) })
            .then(response => response.json())
            .then(data => {
                alert("Thank you for submitting your details.");
                window.location.reload();
            })
            .catch(error => console.error(error));
    });
} else {
    console.error("scriptUrl is already defined");
}