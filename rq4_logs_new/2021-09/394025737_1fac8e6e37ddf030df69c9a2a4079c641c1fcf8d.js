//es una ñapa esto, una lobotomizacion rapida de la logica del modo normal. si sobran cosas, es normal, no estaba escrita para ser asi

let tu_game_board
let su_game_board
let arrayVisionIA
let todasTusCasillas = []
let letrasGriegas = " \u03B1\u03B2\u03B3\u03B4\u03B5\u03B6\u03B7\u03B8\u03D1\u03B9\u03BA"
let numRomanos = [" ", "I", "II", "III", "Iv", "v", "vI", "vII", "vIII", "Ix", "x"]
let trebuchetS = new Audio("trebuchet.mp3")
let aguaS = new Audio("agua.mp3")
let tocadoS = new Audio("tocado.mp3")
let hundidoS = new Audio("hundido.mp3")
let BSO = new Audio("01 Main Theme.mp3")
let victoriaS = new Audio("victoria.mp3")
let derrotaS = new Audio("derrota.mp3")
let civPJ
let civIA
let civilizaciones = ["grecia", "corea", "vikingos"]
let inicialN
let inicialNIA
let direccion = 0 //0 horizontal,1 vertical
let VoH = ["V", "H"];
let barcoInsignia
let casillasOcupadas = []
let IDocupadas = []
let barcoNum = 0
let turn = "jugador"
let turnoNumber = 1
let modo = "buscando"
let posicionesTusBarcos={50:[],40:[],31:[],32:[],21:[],22:[]}
let barcosIMG = {
    5: { vk: ["longboatH.png", "longboatV.png"], G: ["trirremoH.png", "trirremoV.png"], K: ["tortugaH.png", "tortugaV.png"] },
    4: ["transporteH.png", "transporteV.png"],
    3: ["scoutH.png", "scoutV.png"],
    2: ["canoaH.png", "canoaV.png"]
}
let casillasMediasIA = { 50: null, 40: null, 31: null, 32: null, 21: null, 22: null }

//exclusión de casillas para colocar "centro" de barcos
let horizontales5NO = [0, 1, 10, 11, 20, 21, 30, 31, 40, 41, 50, 51, 60, 61, 70, 71, 80, 81, 90, 91, 8, 9, 18, 19, 28, 29, 38, 39, 48, 49, 58, 59, 68, 69, 78, 79, 88, 89, 98, 99]
let verticales5NO = range(0, 20).concat(range(80, 100))
let horizontales4NO = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 8, 9, 18, 19, 28, 29, 38, 39, 48, 49, 58, 59, 68, 69, 78, 79, 88, 89, 98, 99]
let verticales4NO = range(0, 10).concat(range(80, 100))
let horizontales3NO = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 9, 19, 29, 39, 49, 59, 69, 79, 89, 99]
let verticales3NO = range(0, 10).concat(range(90, 100))
let horizontales2NO = [9, 19, 29, 39, 49, 59, 69, 79, 89, 99]
let verticales2NO = range(90, 100)

//range generator for the game (because i love python's range(), does the same thing)

function range(start, stop, step) {
    if (typeof stop == 'undefined') {
        // one param defined
        stop = start;
        start = 0;
    }

    if (typeof step == 'undefined') {
        step = 1;
    }

    if ((step > 0 && start >= stop) || (step < 0 && start <= stop)) {
        return [];
    }

    var result = [];
    for (var i = start; step > 0 ? i < stop : i > stop; i += step) {
        result.push(i);
    }

    return result;
};


//pinta el tablero y genera un board en la memoria

function tablero(box) {
    // crea el board visual
    let visual_board = "";
    for (let i = 0; i < 11; i++) {
        if (i === 0) { visual_board += `<tr><td class="numero">${numRomanos[0]}</td>`; } else { visual_board += `<tr><td class="letra">${letrasGriegas[i]}</td>`; }
        let index = 0
        for (let j = 0; j < 10; j++) {
            if (i === 0) {
                visual_board += `<td class="numero">${numRomanos[j+1]}</td>`;
            } else if (i > 0) {
                if (box === "suGrid") {
                    visual_board += `<td><button id="${index+(10*(i-1))}IA" class="row${i-1} col${j}" onclick="disparo(this)"></button></td>`;
                    index++
                } else {
                    visual_board += `<td><button id="${index+(10*(i-1))}" class="row${i-1} col${j}" onmouseover="colocandoBarco(this)" onclick="despliegue(this)"></button></td>`;
                    index++
                }
            }
        }
        visual_board += "</tr>";
    }

    document.getElementById(box).innerHTML = visual_board;
    document.getElementById(box).style.display = "block";
    // Pinta el board oculto
    // con un array bidimensional
    let idCasilla = 0
    if (box === "tuGrid") {
        tu_game_board = new Array(10);
        for (let i = 0; i < tu_game_board.length; i++) {
            tu_game_board[i] = new Array(10);

            for (let j = 0; j < tu_game_board[i].length; j++) {
                tu_game_board[i][j] = { barco: 0, tocado: 0, casillaGrafica: document.getElementById(idCasilla) };
                todasTusCasillas.push(tu_game_board[i][j])
                idCasilla++;
            }
        }
    } else {
        idCasilla = 0
        su_game_board = new Array(10);
        idCasilla = 0;
        for (let i = 0; i < su_game_board.length; i++) {
            su_game_board[i] = new Array(10);

            for (let j = 0; j < su_game_board[i].length; j++) {
                su_game_board[i][j] = { barco: 0, tocado: 0, sentido: "", casillaGrafica: document.getElementById(`${idCasilla}IA`) };
                idCasilla++;
            }
        }
        //cómo va viendo la partida la IA
        idCasilla = 0
        arrayVisionIA = new Array(10);
        for (let i = 0; i < arrayVisionIA.length; i++) {
            arrayVisionIA[i] = new Array(10);
            for (let j = 0; j < arrayVisionIA[i].length; j++) {
                arrayVisionIA[i][j] = { barco: null, tocado: 0, casillaGrafica: document.getElementById(idCasilla) };
                idCasilla++;
            }
        }
    }
}

//función del disparo con movimiento, trebuchet y efectos 
function disparo(casilla) {
    casilla.disabled = true;
    trebuchetS.play();
    //obtenemos coordenadas de la casilla pulsada
    let objetivo = casilla.getBoundingClientRect();
    let casillaTop = objetivo.top;
    let casillaLeft = objetivo.left;
    //hace aparecer y desaparecer el trebuchet
    document.getElementById("trebuchet").style.display = "block";
    setTimeout(() => {
        document.getElementById("trebuchet").style.display = "none";
    }, 1000);
    //hace aparecer el proyectil y que se mueva al sitio
    setTimeout(() => {
        document.getElementById("fireball").style.display = "block";
        document.getElementById("fireball").animate([
            // keyframes
            { left: `${casillaLeft}px`, top: `${casillaTop}px` }
        ], {
            // timing options
            duration: 500,
        });
    }, 500);
    //hace desaparecer el proyectil
    setTimeout(() => {
        document.getElementById("fireball").style.display = "none";
        hitOrMiss(casilla, su_game_board);
        casilla.style.backgroundImage = "none";
        //check si ha ganado
        if (contBarcosTocadosIA.length === 19) {
            gameEnd()
        } else {
            //llama a disparar a la IA
            document.getElementById("suGrid").style.pointerEvents = "none"
            setTimeout(() => {
                disparoIA()
            }, 500);
        }
    }, 1000);
}

//Dependiendo de la civilización escogida hace una cosa u otra
function civChosen(eleccion) {
    civPJ = eleccion
    //escoge aleatoriamente una civ para la IA
    civilizaciones.splice(civilizaciones.indexOf(eleccion), 1);
    civIA = civilizaciones[Math.floor(Math.random() * civilizaciones.length)];
    if (civIA === "grecia") { inicialNIA = "G" };
    if (civIA === "corea") { inicialNIA = "K" };
    if (civIA === "vikingos") { inicialNIA = "vk" };
    if (eleccion === "grecia") {
        document.getElementById("escogedor").style.display = "none";
        let jingle = new Audio("byzantines.mp3");
        jingle.play();
        let img = document.createElement("img");
        img.src = "trirremoH.png";
        barcoInsignia = img.src
        inicialN = "G"
        document.getElementById('barco5').appendChild(img);
    } else if (eleccion === "corea") {
        document.getElementById("escogedor").style.display = "none";
        let jingle = new Audio("koreans.mp3");
        jingle.play();
        let img = document.createElement("img");
        img.src = "tortugaH.png";
        barcoInsignia = img.src
        inicialN = "K"
        document.getElementById('barco5').appendChild(img);
    } else if (eleccion === "vikingos") {
        document.getElementById("escogedor").style.display = "none";
        let jingle = new Audio("vikings.mp3");
        jingle.play();
        let img = document.createElement("img");
        img.src = "longboatH.png";
        barcoInsignia = img.src
        inicialN = "vk"
        document.getElementById('barco5').appendChild(img);
    }
    tablero("tuGrid")
    document.getElementById("barcos").style.display = "flex";
    setTimeout(() => {
        BSO.play();
    }, 6000);
}

let longitud
let botonPulsado

function barcoSeleccion(barco, IDBarco) {
    if (barco.id === "barco5") {
        longitud = 5;
    } else if (barco.id === "barco4") {
        longitud = 4
    } else if (barco.id === "barco3_1" || barco.id === "barco3_2") {
        longitud = 3
    } else {
        longitud = 2
    }
    barcoNum = IDBarco;
    botonPulsado = barco
}

//enfasis al colocar el barco
let arrayCasillas
let arrayID

function colocandoBarco(casilla) {
    //pone y quita la clase parpadeante al entrar y salir del "centro"
    function ponerQuitar() {
        arrayCasillas.forEach(
            (el) => el.classList.add("desplegando")
        );
        casilla.addEventListener('mouseleave', e => {
            arrayCasillas.forEach(
                (el) => el.classList.remove("desplegando")
            );
        });
    }

    function creaArrayCasillas() {
        arrayCasillas = []
        for (let i = 0; i < arrayID.length; i++) { arrayCasillas.push(document.getElementById(arrayID[i])) }
    }
    //con condicionales para que el barco pueda entrar en las casillas y no se salga por los lados
    if (longitud == 5) {
        if (direccion == 0 && !horizontales5NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id) - 2), (parseInt(casilla.id) - 1), (parseInt(casilla.id)), (parseInt(casilla.id) + 1), (parseInt(casilla.id) + 2)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        } else if (direccion == 1 && !verticales5NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id) - 20), (parseInt(casilla.id) - 10), (parseInt(casilla.id)), (parseInt(casilla.id) + 10), (parseInt(casilla.id) + 20)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        }
    } else if (longitud == 4) {
        if (direccion == 0 && !horizontales4NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id) - 1), (parseInt(casilla.id)), (parseInt(casilla.id) + 1), (parseInt(casilla.id) + 2)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        } else if (direccion == 1 && !verticales4NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id) - 10), (parseInt(casilla.id)), (parseInt(casilla.id) + 10), (parseInt(casilla.id) + 20)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        }
    } else if (longitud == 3) {
        if (direccion == 0 && !horizontales3NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id) - 1), (parseInt(casilla.id)), (parseInt(casilla.id) + 1)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        } else if (direccion == 1 && !verticales3NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id) - 10), (parseInt(casilla.id)), (parseInt(casilla.id) + 10)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        }
    } else if (longitud == 2) {
        if (direccion == 0 && !horizontales2NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id)), (parseInt(casilla.id) + 1)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        } else if (direccion == 1 && !verticales2NO.includes(parseInt(casilla.id))) {
            arrayID = [(parseInt(casilla.id)), (parseInt(casilla.id) + 10)];
            creaArrayCasillas();
            if (!findCommonElements(IDocupadas, arrayID)) {
                ponerQuitar();
            }
        }
    }

    //cambia la dirección de despliegue del barco horizontal/vertical
    document.onkeydown = direccionCambio;

    function direccionCambio(e) {
        if (e.keyCode == '82') {
            if (direccion == 1) {
                direccion = 0;
                arrayCasillas.forEach(
                    (el) => el.classList.remove("desplegando")
                );;
                colocandoBarco(casilla)
            } else {
                direccion = 1;
                arrayCasillas.forEach(
                    (el) => el.classList.remove("desplegando")
                );;
                colocandoBarco(casilla)
            }
        }
    }
}

//coloca el barco gráficamente y en el array del board
function despliegue(casilla) {
    let sentido
    //pone las casillas que ocupa el barco en el board de la memoria
    function inputToBoard() {
        for (let i = 0; i < arrayCasillas.length; i++) {
            //la fila
            let fila = parseInt(arrayCasillas[i].classList[0].slice(-1))
            //la columna
            let columna = parseInt(arrayCasillas[i].classList[1].slice(-1))
            tu_game_board[fila][columna].barco = barcoNum
            //lo pone para que la IA pueda leer luego 
            posicionesTusBarcos[barcoNum].push(tu_game_board[fila][columna])
        }
        //añade a lista de casillas ocupadas
        updateOcupadas()
        //disable botón del barco pulsado
        longitud = 0;
        botonPulsado.style.filter = "grayscale(100%)";
        botonPulsado.disabled = true;
    }
    //pone el barco en ambos boards
    function creaBarco(barco) {
        let img = document.createElement("img");
        img.src = barco;
        //para el caso especial del barco insignia
        if (longitud == 5) {
            img.classList.add(`barcoColocado${longitud}${sentido}${inicialN}`);
        }
        //para los barcos genericos
        else { img.classList.add(`barcoColocado${longitud}${sentido}`); }
        document.getElementById(casilla.id).appendChild(img);
        inputToBoard();
    }

    //con condicionales para que el barco pueda entrar en las casillas y no se salga por los lados
    if (longitud == 5) {
        if (direccion == 0 && !horizontales5NO.includes(parseInt(casilla.id))) {
            sentido = "H"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco(barcoInsignia);
            }
        } else if (direccion == 1 && !verticales5NO.includes(parseInt(casilla.id))) {
            sentido = "V"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco(barcoInsignia.replace("H", "V"));
            }
        }
    } else if (longitud == 4) {
        if (direccion == 0 && !horizontales4NO.includes(parseInt(casilla.id))) {
            sentido = "H"
            creaBarco("transporteH.png");
        } else if (direccion == 1 && !verticales4NO.includes(parseInt(casilla.id))) {
            sentido = "V"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco("transporteV.png");
            }
        }
    } else if (longitud == 3) {
        if (direccion == 0 && !horizontales3NO.includes(parseInt(casilla.id))) {
            sentido = "H"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco("scoutH.png");
            }
        } else if (direccion == 1 && !verticales3NO.includes(parseInt(casilla.id))) {
            sentido = "V"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco("scoutV.png");
            }
        }
    } else if (longitud == 2) {
        if (direccion == 0 && !horizontales2NO.includes(parseInt(casilla.id))) {
            sentido = "H"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco("canoaH.png");
            }
        } else if (direccion == 1 && !verticales2NO.includes(parseInt(casilla.id))) {
            sentido = "V"
            if (!findCommonElements(IDocupadas, arrayID)) {
                creaBarco("canoaV.png")
            }
        }
    }
}

//actualiza lista de casillas ocupadas y llama a crear el board enemigo si se ha colocado todo
function updateOcupadas() {
    for (let i = 0; i < tu_game_board.length; i++) {
        for (let j = 0; j < tu_game_board[i].length; j++) {
            if ((!(tu_game_board[i][j].barco == 0)) && !(casillasOcupadas.includes(tu_game_board[i][j]))) {
                casillasOcupadas.push(tu_game_board[i][j])
                IDocupadas.push(parseInt(tu_game_board[i][j].casillaGrafica.id))
            }
        }
    }
    if (IDocupadas.length == 19) {
        document.getElementById("barcos").style.display = "none";
        tablero("suGrid")
        longitud = 5
        enemigoDespliegue()
    }
}

//encuentra si dos arrays tienen algún elemento en común
function findCommonElements(arr1, arr2) {
    return arr1.some(item => arr2.includes(item))
}
//encuentra si todos los elementos de un array son 0
function todoCeros(array) {
    return array.every(item => item === 0);
}
//devuelve un elemento aleatorio de un array
function randomElementArr(array) {
    return array[Math.floor(Math.random() * array.length)];
}
//devuelve un elemento aleatorio de un array excluyendo algunos elementos
function randomExcludingItems(array, excludeIndexArray) {
    let randIndex = excludeIndexArray[0];
    while (excludeIndexArray.includes(randIndex)) {
        randIndex = Math.floor(Math.random() * array.length)
    }
    return array[randIndex];
}
//devuelve int aleatorio entre 2 numeros ambos inclusivos
function getRandomArbitrary(min, max) {
    return Math.floor(Math.random() * (max - min + 1) + min);
}
//cuenta el número de veces que se repite un elemento en un array
function getOccurrence(array, value) {
    return array.filter((v) => (v === value)).length;
}
//devuelve true si es par
function esPar(value) {
    if (value % 2 == 0)
        return true;
    else
        return false;
}

let dosBarcos = 1

//halla la casilla "media" del barco de la IA
function casillaMedia(IDBarco, centro, i, PrimCasilla) {
    if (IDBarco === 50) { if (i === PrimCasilla + 2) { casillasMediasIA[50] = centro } } else if (IDBarco === 40) { if (i === PrimCasilla + 1) { casillasMediasIA[40] = centro } } else if (IDBarco === 31) { if (i === PrimCasilla + 1) { casillasMediasIA[31] = centro } } else if (IDBarco === 32) { if (i === PrimCasilla + 1) { casillasMediasIA[32] = centro } } else if (IDBarco === 21) { if (i === PrimCasilla) { casillasMediasIA[21] = centro } } else if (IDBarco === 22) { if (i === PrimCasilla) { casillasMediasIA[22] = centro } }
}

//posiciona barcos del enemigo
function enemigoDespliegue() {
    console.log("se llama al despliegue")
    let dirTemp = randomElementArr(VoH);
    let tempPosition
    let fila
    let columna
    let PrimCasilla
    let UltCasilla
    //comprueba si está libre y coloca
    function checkIfFree(direction, IDBarco) {
        console.log("se chequea")
        PrimCasilla = getRandomArbitrary(0, (10 - longitud));
        UltCasilla = PrimCasilla + longitud;
        if (direction === "H") {
            fila = getRandomArbitrary(0, 9);
            tempPosition = [];
            for (let i = PrimCasilla; i < UltCasilla; i++) {
                tempPosition.push(su_game_board[fila][i].barco)
            }
            if (todoCeros(tempPosition)) {
                for (let i = PrimCasilla; i < UltCasilla; i++) {
                    su_game_board[fila][i].barco = IDBarco;
                    su_game_board[fila][i].sentido = "H";
                    casillaMedia(IDBarco, su_game_board[fila][i], i, PrimCasilla);
                }
                console.log(`se ha hecho el ${PrimCasilla} en longitud ${longitud} y direccion ${direction}`);
                dirTemp = randomElementArr(VoH);
                longitud--
                if (dosBarcos === 1 && longitud < 3) {
                    dosBarcos++;
                    longitud++;
                } else if (dosBarcos === 2) { dosBarcos-- }
                if (longitud > 1) { enemigoDespliegue() }
            } else { enemigoDespliegue() }
        } else {
            columna = getRandomArbitrary(0, 9);
            tempPosition = [];
            for (let i = PrimCasilla; i < UltCasilla; i++) {
                tempPosition.push(su_game_board[i][columna].barco);
            }
            if (todoCeros(tempPosition)) {
                for (let i = PrimCasilla; i < UltCasilla; i++) {
                    su_game_board[i][columna].barco = IDBarco;
                    su_game_board[i][columna].sentido = "V";
                    casillaMedia(IDBarco, su_game_board[i][columna], i, PrimCasilla);
                }
                console.log(`se ha hecho el ${PrimCasilla} en longitud ${longitud} y direccion ${direction}`);
                dirTemp = randomElementArr(VoH);
                longitud--
                if (dosBarcos === 1 && longitud < 3) {
                    dosBarcos++;
                    longitud++;
                } else if (dosBarcos === 2) { dosBarcos-- }
                if (longitud > 1) { enemigoDespliegue() }
            } else { enemigoDespliegue() }
        }
    }

    switch (longitud) {
        case 5:
            checkIfFree(dirTemp, 50);
            break;

        case 4:
            checkIfFree(dirTemp, 40);
            break;

        case 3:
            if (dosBarcos === 1) { checkIfFree(dirTemp, 31) } else { checkIfFree(dirTemp, 32) }
            break;

        case 2:
            if (dosBarcos === 1) { checkIfFree(dirTemp, 21); } else { checkIfFree(dirTemp, 22); }
            break;
    }
}

let contBarcosTocados = []
let contBarcosTocadosIA = []

//devuelve la primera cifra, para saber la longitud del barco acertado y determinar si está hundido, los barcos son 50,40,31,32,21,22
function primeraCifra(numero) {
    return Number(String(numero).charAt(0));
};
//crea fuego en la casilla
function prender(casilla) {
    let img = document.createElement("img");
    img.style.width = "8vh";
    img.style.height = "8vh";
    img.style.zIndex = "15"
    img.style.position = "absolute";
    img.style.transform = "translate(-4vh,-5vh)"
    img.src = "fireball.gif";
    document.getElementById(casilla.id).appendChild(img);
}
//revela el barco hundido
function mostrarBarco(valorBarco, fila, columna) {
    let img = document.createElement("img");
    longitud = primeraCifra(valorBarco);
    sentido = su_game_board[fila][columna].sentido;
    let barco
    //para el caso especial del barco insignia
    if (longitud == 5) {
        if (sentido === "H") { barco = barcosIMG[longitud][inicialNIA][0] } else if (sentido === "V") { barco = barcosIMG[longitud][inicialNIA][1] }
        img.src = barco;
        img.classList.add(`barcoColocado${longitud}${sentido}${inicialNIA}`);
    }
    //para los barcos genericos
    else {
        if (sentido === "H") { barco = barcosIMG[longitud][0] } else if (sentido === "V") { barco = barcosIMG[longitud][1] }
        img.src = barco;
        img.classList.add(`barcoColocado${longitud}${sentido}`);
    }
    casillasMediasIA[valorBarco].casillaGrafica.appendChild(img);
}

//Comprueba si es agua, tocado o hundido
function hitOrMiss(casilla, board) {
    //la fila
    let fila = casilla.classList[0].slice(-1)
    //la columna
    let columna = casilla.classList[1].slice(-1)
    //si es agua
    if (board[fila][columna].barco === 0) {
        aguaS.play();
        if (turn === "AI") {
            //barco 0 es si es agua
            arrayVisionIA[fila][columna].barco = 0
        }
    }
    //si es acierto
    else {
        //tocado 1 es un tocado. tocado 2 es hundido
        if (turn === "AI") {
            arrayVisionIA[fila][columna].barco = board[fila][columna].barco
            board[fila][columna].tocado = 1
            let valorBarco = board[fila][columna].barco
            barcoTocado=valorBarco
            posicionBarcoTocado=posicionesTusBarcos[barcoTocado]
            posicionBarcoTocado.splice(posicionBarcoTocado.indexOf(eleccionIA),1)
            contBarcosTocados.push(valorBarco)
            if (getOccurrence(contBarcosTocados, valorBarco) < primeraCifra(valorBarco)) {
                ultimoDisparoID=parseInt(`${fila}${columna}`)
                tocadoS.play();
                prender(casilla)
                modo = "buscando"
                aciertos++
            } else {
                board[fila][columna].tocado = 2
                hundidoS.play();
                prender(casilla)
                aciertos++
                primerAciertoID=-1
                modo = "buscando"
            }
        } else {
            board[fila][columna].tocado = 1
            let valorBarco = board[fila][columna].barco
            contBarcosTocadosIA.push(valorBarco)
            if (getOccurrence(contBarcosTocadosIA, valorBarco) < primeraCifra(valorBarco)) {
                tocadoS.play();
                prender(casilla);
            } else {
                board[fila][columna].tocado = 2
                hundidoS.play();
                prender(casilla);
                mostrarBarco(valorBarco, fila, columna);
            }
        }
    }
}

let eleccionIA
//IA disparo
function disparoIA() {
    turn = "AI"
    //IA piensa y dispara
    eleccionIA = IA()
    trebuchetS.play();
    //obtenemos coordenadas de la casilla pulsada
    let objetivo = eleccionIA.casillaGrafica.getBoundingClientRect();
    let casillaTop = objetivo.top;
    let casillaLeft = objetivo.left;
    //hace aparecer y desaparecer el trebuchet
    document.getElementById("trebuchetIA").style.display = "block";
    setTimeout(() => {
        document.getElementById("trebuchetIA").style.display = "none";
    }, 1000);
    //hace aparecer el proyectil y que se mueva al sitio
    setTimeout(() => {
        document.getElementById("fireballIA").style.display = "block";
        document.getElementById("fireballIA").animate([
            // keyframes
            { left: `${casillaLeft}px`, top: `${casillaTop}px` }
        ], {
            // timing options
            duration: 500,
        });
    }, 500);
    //hace desaparecer el proyectil
    setTimeout(() => {
        document.getElementById("fireballIA").style.display = "none";
        hitOrMiss(eleccionIA.casillaGrafica, tu_game_board);
        //check si ha ganado
        if (contBarcosTocados.length === 19) {
            gameEnd()
        } else {
            //deja volver a disparar al jugador
            document.getElementById("suGrid").style.pointerEvents = "auto"
            setTimeout(() => {
                turn = "jugador"
            }, 500);
        }
    }, 1000);
}

//game end
function gameEnd() {
    BSO.pause();
    let civVic
    let texto
    if (turn === "jugador") {
        civVic = civPJ
        victoriaS.play();
        texto = "victoria"
    } else {
        civVic = civIA
        derrotaS.play();
        texto = "derrota"
        document.getElementById("ganador").style.color = "rgb(127, 0, 0)"
        derrotaS.play();
    }

    document.getElementById("ganadorFoto").src = `${civVic}victoria.jpg`
    document.getElementById("fin").style.display = "flex"
    document.getElementById("ganador").innerText = texto
}
let ultimoDisparoID
let indicesUsadosBuscando=[]
//el array de indices que salta casillas una si una no. me duele la cabeza y no consigo pensar en una forma de crearlo que me lleve menos tiempo que escribirlo a mano
let indicesUsadosBuscandoDos = [1, 3, 5, 7, 9, 10, 12, 14, 16, 18, 21, 23, 25, 27, 29, 30, 32, 34, 36, 38, 41, 43, 45, 47, 49, 50, 52, 54, 56, 58, 61, 63, 65, 67, 69, 70, 72, 74, 76, 78, 81, 83, 85, 87, 89, 90, 92, 94, 96, 98]
//para encontrar los de 3
let indicesUsadosBuscandoTres = [1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19, 20, 22, 23, 25, 26, 28, 29, 31, 32, 34, 35, 37, 38, 40, 41, 43, 44, 46, 47, 49, 50, 52, 53, 55, 56, 58, 59, 61, 62, 64, 65, 67, 68, 70, 71, 73, 74, 76, 77, 79, 80, 82, 83, 85, 86, 88, 89, 91, 92, 94, 95, 97, 98]
//para encontrar el de 4
let indicesUsadosBuscandoCuatro = [1, 2, 3, 5, 6, 7, 9, 10, 12, 13, 14, 16, 17, 18, 20, 21, 23, 24, 25, 27, 28, 29, 30, 31, 32, 34, 35, 36, 38, 39, 41, 42, 43, 45, 46, 47, 49, 50, 52, 53, 54, 56, 57, 58, 60, 61, 63, 64, 65, 67, 68, 69, 70, 71, 72, 74, 75, 76, 78, 79, 81, 82, 83, 85, 86, 87, 89, 90, 92, 93, 94, 96, 97, 98]
//para encontrar el de 5. ya me volví listo
let indicesUsadosBuscandoCinco = (range(0, 100)).filter(val => ![0, 5, 11, 16, 22, 27, 33, 38, 44, 49, 50, 55, 61, 66, 72, 77, 83, 88, 94, 99].includes(val));

function IA() {
    if (turnoNumber === 1) {
        //empieza en una casilla "par" y se han excluido ya las casillas "impares" para el modo búsqueda
        let aperturas = [tu_game_board[4][4], tu_game_board[5][5]]
        turnoNumber++
        let eleccion = randomElementArr(aperturas)
        //añade el índice de la elección a los índices usados para el modo buscando
        indicesUsadosBuscando.push(todasTusCasillas.indexOf(eleccion))
        indicesUsadosBuscando = [...new Set(indicesUsadosBuscando)]
        indicesUsadosBuscandoDos.push(todasTusCasillas.indexOf(eleccion))
        indicesUsadosBuscandoDos = [...new Set(indicesUsadosBuscandoDos)]
        //pone el filtro en el barco de dos
        arrayDelMomento = indicesUsadosBuscandoDos
        arrayDelMomento.concat(indicesUsadosBuscando)
        //pone un 0 en el lugar de la casilla que ya se ha utilizado
        todasTusCasillas.splice(todasTusCasillas.indexOf(eleccion), 1, 0)
        ultimoDisparoID = eleccion.casillaGrafica.id
        return eleccion
    } else {
            turnoNumber++
            return buscando()
        } 
}
let afinandoPunteria = 2
//el array que filtra casillas 
let arrayDelMomento

function buscando() {
    let escogida
    /*esto siempre hace lo mismo. 
    escoge un elemento del array con todas tus casillas, excluyendo los índices usados o que deben saltarse.
    cambia el elemento del array por un 0 para marcar que se ha usado y devuelve la elección
    */
    function eligeLaIA() {
        escogida = randomExcludingItems(todasTusCasillas, arrayDelMomento)
        indicesUsadosBuscando.push(todasTusCasillas.indexOf(escogida))
        arrayDelMomento = arrayDelMomento.concat(indicesUsadosBuscando)
        arrayDelMomento.push(todasTusCasillas.indexOf(escogida))
        //limpiamos de repes el array
        arrayDelMomento = [...new Set(arrayDelMomento)]
        todasTusCasillas.splice(todasTusCasillas.indexOf(escogida), 1, 0)
        ultimoDisparoID = escogida.casillaGrafica.id
        return escogida
    }
    //si quedan barcos de 2 por hundir
    if ((getOccurrence(contBarcosTocados, 21)) + (getOccurrence(contBarcosTocados, 22)) < 4) {
        return eligeLaIA()
    }
    //si los barcos de 2 se han hundido todos
    else if (getOccurrence(contBarcosTocados, 21) === 2 && getOccurrence(contBarcosTocados, 22) === 2) {
        //si es el primer disparo después de haber hundido todos los barcos de 2
        if (afinandoPunteria === 2) {
            //ponemos el array de filtro de 3 a funcionar
            indicesUsadosBuscandoTres.concat(indicesUsadosBuscando)
            arrayDelMomento = indicesUsadosBuscandoTres
            arrayDelMomento = arrayDelMomento.concat(indicesUsadosBuscando)
            afinandoPunteria++
        }
        //si quedan barcos de 3 por hundir
        if ((getOccurrence(contBarcosTocados, 31)) + (getOccurrence(contBarcosTocados, 32)) < 6) {
            return eligeLaIA()
        }
        //si los barcos de 3 se han hundido todos
        else if (getOccurrence(contBarcosTocados, 31) === 3 && getOccurrence(contBarcosTocados, 32) === 3) {
            //si es el primer disparo después de haber hundido todos los barcos de 3
            if (afinandoPunteria === 3) {
                indicesUsadosBuscandoCuatro.concat(indicesUsadosBuscando)
                arrayDelMomento = indicesUsadosBuscandoCuatro
                arrayDelMomento = arrayDelMomento.concat(indicesUsadosBuscando)
                afinandoPunteria++
            }
            //si el barco de 4 no se ha hundido aún
            if ((getOccurrence(contBarcosTocados, 40) < 4)) {
                return eligeLaIA()
            }
            //si se ha hundido todos menos el de 5
            else if (getOccurrence(contBarcosTocados, 40) === 4) {
                //si es el primer disparo después de haber hundido todos los barcos de 3
                if (afinandoPunteria === 4) {
                    indicesUsadosBuscandoCinco.concat(indicesUsadosBuscando)
                    arrayDelMomento = indicesUsadosBuscandoCinco
                    arrayDelMomento = arrayDelMomento.concat(indicesUsadosBuscando)
                    afinandoPunteria++
                }
                return eligeLaIA()
            }
        }
    }

}
let aciertos=0
//lista de id de casillas en las que puede estar
let posibleCasillaID=[]
let barcoTocado
let posicionBarcoTocado
let primerAciertoID=-1


//always music, always good
if (typeof BSO.loop == 'boolean') {
    BSO.loop = true;
} else {
    BSO.addEventListener('ended', function() {
        this.currentTime = 0;
        this.play();
    }, false);
}