window.addEventListener("load", function () {


    // ARRAY DE PALABRAS
    let palabras = ["HOLA", "CARRO", "COMIDA", "AVION", "ANIMAL", "ELEFANTE", "MUNDO", "PLANETA", "BOXEO",
        "CAMPEON", "TIRANO", "ADELANTE", "CAMINO", "MENTIRA", "EDIFICIO", "JIRAFA", "ACORDEON",
        "ESPECIAL", "DINO", "ATRAS", "JABALI", "ALCALDE", "CIUDAD", "ANGEL", "DEMONIO", "AMARILLO", "BUS", "GUIA",
        "JUEGO", "JUGAR", "PERDER", "LEON", "TIGRE", "LORENZO", "LORO", "PERDER", "GANAR", "AMIGO", "AMISTAD",
        "NOVIO", "NOVIA", "ESPOSA", "AMANTE", "ABACO", "ABANICO", "PLANCHA", "MESA", "PUERTA", "TESORO", "PIRATA",
        "PELICULA","DIARIO","AFRICA","BACALAO","TORTUGA","ALFILER","AGUJA","TRACTOR","PERDON","CASTIGO"];
    let random;
    let palabraSecreta;
    let caja = document.querySelector(".caja");


    // BOTONES
    let btnJugar = document.querySelector("#jugar");
    let btnNuevoJuego = document.querySelector("#nuevoJuego");
    let btnVolumen = document.querySelector(".volumen");
    let btnDesistir = document.querySelector("#desistir");


    // DIVS
    let textoBueno = document.querySelector(".letrasBuenas");
    let textoMalo = document.querySelector(".letrasMalas");
    let imagen = document.querySelector(".imagen");


    // CONTROL DE LA MUSICA
    let vApagado = document.querySelector("#apagado");
    let vActivo = document.querySelector("#activo");
    let audio1 = document.querySelector("#audio1");
    let audio2 = document.querySelector("#audio2");
    let audio3 = document.querySelector("#audio3");


    // CONTADORES Y VARIABLES DE CONTROL
    let cont = 0;
    let contWin = 0;
    let juego = true;
    let audio = true;
    let letrasUsadas = [];


    // EXPRESION REGULAR
    const re = /^[A-Z]+$/;


    // METODO PARA ACTIVAR Y DESACTIVAR EL VOLUMEN
    btnVolumen.addEventListener("click", function () {


        if (audio) {
            audio2.pause();
            audio = false;
            vActivo.style.display = "none";
            vApagado.style.display = "block";
        } else {
            audio2.play();
            vActivo.style.display = "block";
            vApagado.style.display = "none";
            audio = true;
        }

    });

    btnDesistir.addEventListener("click", function () {
        location.reload();
    });


    /** FUNCIONES */

    // funcion para generar numero aleatorio
    function getRandomInt(min, max) {
        return Math.floor(Math.random() * (max - min)) + min;
    }

    // funcion que inicia un nuevo juego y restaura los valores de control
    function iniciarJuego() {

        juego = true;
        contWin = 0;
        audio2.play();

        random = getRandomInt(0, palabras.length);
        palabraSecreta = palabras[random];
        caja.style.visibility = "visible"
        btnJugar.style.display = "none";
        textoMalo.textContent = "";
        letrasUsadas = [];
        textoMalo.textContent = "";
        dibujarGuiones();

    }

    // funcion que el contenido anterior
    function eliminarDivs() {
        while (textoBueno.firstChild) {
            textoBueno.removeChild(textoBueno.firstChild);
        }
        imagen.style.backgroundImage = "none";
        imagen.textContent = "";
        cont = 0;
    }

    // funcion para dibujar los guiones en base a la palabra seleccionada
    function dibujarGuiones() {


        for (let x = 0; x < palabraSecreta.length; x++) {
            let caja = dibujarDivs();
            caja.id = "caja" + x;
            textoBueno.appendChild(caja);

        }

    }

    // funcion que dibujas las cajas donde iran las letras de las palabras seleccionadas
    function dibujarDivs() {
        let cajaLetras = document.createElement("div");
        cajaLetras.style.borderBottom = "solid 2px red"
        cajaLetras.style.flex = "1 1 auto";
        cajaLetras.style.margin = "5px";
        cajaLetras.style.height = "80%";
        cajaLetras.style.display = "flex";
        cajaLetras.style.justifyContent = "center";
        cajaLetras.style.alignItems = "center";

        return cajaLetras;
    }


    /** EVENTOS */

    //evento para el boton jugar de la pagina principal que inicia el juego
    btnJugar.addEventListener("click", iniciarJuego);


    // evento del boton nuevo juego de la pagina de juego que inicia un nuevo juego
    btnNuevoJuego.addEventListener("click", function () {
        eliminarDivs();
        iniciarJuego();
    });


    // evento que se activa si el usuario oprime una tecla
    window.addEventListener("keypress", function (event) {
        if (event.defaultPrevented) {
            return;
        }

        let letra = event.key;
        let poscicion = [];


        // juego es una variable booleana que define si el juego acabo, si el juego finalizo
        // toma valor de false, por lo que no entra al control hasta que se inicie un nuevo juego
        // donde tomara el valor de true
        if (juego) {

            // segundo control donde se verifica que la letra oprimida este entre la A y la Z
            // y sea mayuscula, mediante una expresion regular sencilla
            if (re.test(letra)) {


                // Tercer control donde se verifica que la letra no se haya usado,
                // las letras que se van usando se guardan en un array
                if (letrasUsadas.includes(letra) === false) {

                    letrasUsadas.push(letra);


                    // ciclo for para determinar las posiciones de la letra introducida
                    for (let x = 0; x < palabraSecreta.length; x++) {

                        if (palabraSecreta[x] === letra) {
                            poscicion.push(x);

                        }
                    }

                    // ciclo para insertar las letras introducidas en su correspondiente renglon
                    // y se incrementa el marcador de juego ganado
                    for (let y = 0; y < poscicion.length; y++) {

                        let nombre = "" + poscicion[y];
                        let caja = document.querySelector("#caja" + CSS.escape(nombre));
                        caja.textContent = palabraSecreta[poscicion[y]];
                        contWin++;

                    }


                    // en este if se confirma si la letra ingresada esta errada y se dibuja el muñeco
                    // se incrementa el contador de control de juego perdido
                    if (poscicion.length <= 0) {
                        cont++;
                        imagen.style.backgroundImage = "url(\"img/sprits/sprit" + cont + ".png\")";
                        textoMalo.textContent = textoMalo.textContent + letra;

                        // este if comprueba el marcador de juego perdido, y si llega a 11 finaliza el juego
                        if (cont === 11) {
                            imagen.style.backgroundImage = "url(img/sprits/sprit11.png)";
                            imagen.textContent = "PERDISTE";
                            audio1.play();
                            juego = false;
                            letrasUsadas = [];
                            audio3.play();

                            return;
                        }
                        return;

                    }

                    // este if conprueba el marcador de juego ganado, y finaliza el juego
                    // si todos los renglones fueron completados
                    if (palabraSecreta.length === contWin) {

                        imagen.textContent = "FELICIDADES GANASTE";
                        audio1.play();
                        juego = false;
                        return;
                    }
                }
                // en caso de que el usuario introduzac una letra incorrecta se envia una alerta
            } else {
                alert("Solo letras mayusculas sin acento");
            }

        }

        event.preventDefault();
    }, true);


});


