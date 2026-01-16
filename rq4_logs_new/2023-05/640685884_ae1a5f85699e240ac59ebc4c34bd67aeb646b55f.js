(()=>{
    const app={
        htmlElements:{
            fibonacciForm:document.getElementById('fibonacci'),
            fibonacciinputForm:document.getElementById('num'),
            lista_num:document.getElementById('fibon-results'),

        },

        init(){

            //Handlers
            app.htmlElements.fibonacciForm.addEventListener('submit',
            app.handlers.fibonacciFormHandler);
            
        },

        handlers:{
            fibonacciFormHandler(event){
                event.preventDefault();
                const num=app.htmlElements.fibonacciinputForm.value;
                const respuesta=app.methods.function_fibonacci({num});
                
                //llamado al metodo para imprimir la resp
                app.methods.function_resp_fib({respuesta});

            }
        },

        methods:{
            function_fibonacci({num}){
                var fibonacci=[];
                fibonacci[0] = 0;
                fibonacci[1] = 1;
                for (var i = 2; i < num; i++) {
                fibonacci[i] = fibonacci[i - 2] + fibonacci[i - 1];
                } 
                return fibonacci;
            },
            function_resp_fib({respuesta}){
                const lista_num=app.htmlElements.lista_num;
                lista_num.innerHTML=" ";
                respuesta.forEach((fibonacci)=>{
                    const elemento=document.createElement('div');
                    elemento.textContent=fibonacci;
                    elemento.classList.add('grupo_num');
                    lista_num.appendChild(elemento);

                    const cerrar=document.createElement('div');
                    cerrar.textContent="x";
                    cerrar.classList.add('grupo_close');
                //    const cerrado_event=cerrar.getElementsByTagName('onclick="app.parentElement.style.display="none""');
                //    const evento=cerrado_event.getElementsByTagName('div');
                //    document.getElementById('grupo_close').innerHTML=evento.innerHTML;
                    lista_num.appendChild(cerrar);
                });
            }
        }
    }
        app.init();
})();