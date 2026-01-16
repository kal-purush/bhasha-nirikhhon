

function noReloadTab (){

    const form = document.querySelector('.formCalc');
    const res = document.querySelector('.htmlAnswer');
    
    const openModalButtons = document.querySelectorAll('[data-modal-target]');
    const closeModalButtons = document.querySelectorAll('[data-close-button]');
    const overlay = document.getElementById('overlay');

    console.log('Entrei na função main');


    function receiveEvent(event) {
        event.preventDefault();

        console.log('Entrei na função Stop');


    //formula do IMC = PESO / ALTURA X ALTURA

            const peso = form.querySelector('.peso');

            console.log(peso.value);

            const altura = form.querySelector('.altura');

            console.log(altura.value);

            let calc = peso.value / ( altura.value * altura.value);
            
        

            if(calc < 18.5){
                res.innerHTML = `<p>${calc.toFixed(2)}` + ` Abaixo do peso</p>`;
            }
            
            if (calc >= 18.5 && calc <= 24.9){
                
                res.innerHTML = `<p>${calc.toFixed(2)}` + ` Peso normal</p>`;
            }
            if (calc >= 25 && calc <= 29.9){
                
                res.innerHTML = `<p>${calc.toFixed(2)}` + ` Sobrepeso</p>`;
            }
            if (calc >= 30 && calc <= 34.9){
                
                res.innerHTML = `<p>${calc.toFixed(2)}` + ` Obesidade grau 1</p>`;
            }
            if (calc >= 35 && calc <= 39.9){
                
                res.innerHTML = `<p>${calc.toFixed(2)}` + ` Obesidade grau 2</p>`;
            }
            
            if( calc >= 40) {
                
                res.innerHTML = `<p>${calc.toFixed(2)}` + ` Obesidade grau 3</p>`;
            }
            
      
  
    }

    form.addEventListener('submit', receiveEvent);
    

    overlay.addEventListener('click', () => {
        const modals = document.querySelectorAll('.popUpBox.active');
        modals.forEach(modal => {
            closeModal(modal);
        })
    })


    openModalButtons.forEach(button => {
        button.addEventListener('click', () => {
            const modal = document.querySelector(button.dataset.modalTarget);
            openModal(modal);
        })
    })

    function openModal (modal){
        if (modal == null) return 
        modal.classList.add('active');
        overlay.classList.add('active');
    }

    
    closeModalButtons.forEach (button => {
        button.addEventListener('click', () => {
            const modal = button.closest('.popUpBox');
            closeModal(modal);
        })
    })


    function closeModal (modal) {
        console.log("Entrei na função closeModal");
        if (modal == null) return 
        modal.classList.remove('active');
        overlay.classList.remove( 'active');
    }


}

noReloadTab();
