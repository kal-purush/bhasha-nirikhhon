import React from 'react'

export default function FooterHome() {
    console.log(
       'HEIG', window.screen.availHeight,
        'width',window.screen.availWidth
    );
    return (
        <div id="footerHome">
            <p id="textFooter"><strong>
                Interlag Education & Travel                     
            </strong>
            &nbsp;ofrece la más alta calidad <br></br>en servicios educativos y los mejores programas 
            para estudiar en el extranjero.
            </p>
            <h1 id="since">desde 1994</h1>
        </div>
    )
}