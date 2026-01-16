// VARIABLES RELACIONADAS CON PAGINADOR
// let g_numPagActual = 1;
// let g_numPagTotal = 1;
let g_paginadorTipo1 = _createTipoPaginador("border-1px-azulPrimary","text-azulPrimary", "font-size-15");
let g_paginadorTipo2= _createTipoPaginador("border-1px-azulDeepKoamaru","text-azulDeepKoamaru","font-size-12");

// Funcion que crea tipo de paginadores en materia de color, y tamanyo de fuente
function _createTipoPaginador(...p_array){
    let arrayResul = [];

    for(elem of p_array){
        arrayResul.push(elem);
    }

    return arrayResul;
}



class Paginador{

    /* -- PROPIEDADES -- */
    // this.l_border;
    // this.l_textColor;
    // this.l_tamanyoFuente;
    // this.l_nodoPadre;
    // this.l_posicionEnNodoPadrePaginador;
    // this.l_nodoPadreElementosPaginados;
    // this.l_posicionEnNodoPadreElementosPaginados;
    // this.l_paginaActual;
    // this.l_numPagTotales;
    // this.l_numElementosXPag;
    // this.l_arrayTotal;
    // this.l_arrayTramo;
    // this.l_plantillaPaginadorHTML;


    /* ----- CONSTRUCTOR ----- */
    constructor(p_nodoPadrePaginador,p_nodoPadreElementosPaginados,p_arrayTotal,p_numElementosXPag = 1,p_border = "border-1px-azulPrimary",p_textColor = "text-azulPrimary",p_tamanyoFuente = "font-size-15",p_posicionPaginadorEnNodoPadre = "beforeend",p_posicionElementosPaginados="beforeend"){

        // ESTILO
        this.setBorder(p_border);
        this.setTextColor(p_textColor);
        this.setTamanyoFuente(p_tamanyoFuente);


        // NODO PADRE DE PAGINADOR
        this.setNodoPadrePaginador(p_nodoPadrePaginador);
        this.setPosicionEnNodoPadrePaginador(p_posicionPaginadorEnNodoPadre);

        // NODO PADRE ELEMENTOS PAGINADOS
        this.setNodoPadreElementosPaginados(p_nodoPadreElementosPaginados);
        this.setPosicionEnNodoPadreElementosPaginados(p_posicionElementosPaginados);

        //PAGINAS
        // -- paginas totales --
        this.setNumPagTotales(Math.ceil(p_arrayTotal.length/p_numElementosXPag));
        // -- pagina actual --
        this.setNumPagActual(1);

        // NUMERO DE ELEMENTOS POR PAGINA
        this.setNumElementosXPagina(p_numElementosXPag);

        // ARRAY
        this.setArrayTotal(p_arrayTotal);
        this.setArrayTramo(p_arrayTotal,p_numElementosXPag,1);

        // EVENTO 'CLICK' PIE PAGINA
        this.clickNumPagina();

    }

    /* ------------------ GETTERS AND SETTERS ------------------ */
    //GET CLASE CSS PARA PAGINA ACTUAL
    getCssClassActiveNumPag(){
        return "activeNumPage";
    }

    //GET y SET  BORDE
    getBorder(){
        return this.l_border;
    }
    setBorder(p_border){
        
        if( _validaNoNullNoUndefined(p_border) && _validaCadenaNoVacia(p_border) ){
            // actualiza valor de su propiedad
            this.l_border = p_border;

            if( _validaNoNullNoUndefined(this.getNumPagActual()) && _validaNoNullNoUndefined(this.getNumPagTotales()) ){
                // actualiza la plantilla HTML del paginador
                this.privateUpdatePlantillaPaginadorHTML(this.getNumPagActual(),this.getNumPagTotales());
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    //GET y SET  TEXT COLOR
    getTextColor(){
        return this.l_textColor;
    }
    setTextColor(p_textColor){

        if( _validaNoNullNoUndefined(p_textColor) && _validaCadenaNoVacia(p_textColor) ){
            // actualiza valor de su propiedad
            this.l_textColor = p_textColor;

            if( _validaNoNullNoUndefined(this.getNumPagActual()) && _validaNoNullNoUndefined(this.getNumPagTotales()) ){
                // actualiza la plantilla HTML del paginador
                this.privateUpdatePlantillaPaginadorHTML(this.getNumPagActual(),this.getNumPagTotales());
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    //GET y SET TAMANYO DE FUENTE
    getTamanyoFuente(){
        return this.l_tamanyoFuente;
    }
    setTamanyoFuente(p_tamanyoFuente){

        if( _validaNoNullNoUndefined(p_tamanyoFuente) && _validaCadenaNoVacia(p_tamanyoFuente) ){
            // actualiza valor de su propiedad
            this.l_tamanyoFuente = p_tamanyoFuente;

            if( _validaNoNullNoUndefined(this.getNumPagActual()) && _validaNoNullNoUndefined(this.getNumPagTotales()) ){
                // actualiza la plantilla HTML del paginador
                this.privateUpdatePlantillaPaginadorHTML(this.getNumPagActual(),this.getNumPagTotales());
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    //GET y SET NODO PADRE DE PAGINADOR
    getNodoPadrePaginador(){
        return this.l_nodoPadre;
    }
    setNodoPadrePaginador(p_nodoPadre){
        if(_validaNoNullNoUndefined(p_nodoPadre)){
            // actualiza valor de su propiedad
            this.l_nodoPadre = p_nodoPadre;
            // dibuja los elementos y el paginador
            this.dibuja();
        }
    }

    //GET y SET  POSICION EN EL NODO PADRE PAGINADOR
    getPosicionEnNodoPadrePaginador(){
        return this.l_posicionEnNodoPadrePaginador;
    }
    setPosicionEnNodoPadrePaginador(p_posicionEnNodoPadrePaginador){
        if( _validaNoNullNoUndefined(p_posicionEnNodoPadrePaginador) && _validaCadenaNoVacia(p_posicionEnNodoPadrePaginador) && _validaPosicionInsertAdjacentHTML(p_posicionEnNodoPadrePaginador) ){
            // actualiza valor de su propiedad
            this.l_posicionEnNodoPadrePaginador = p_posicionEnNodoPadrePaginador;
            // dibuja los elementos y el paginador
            this.dibuja();
        }
    }

    //GET y SET NODO PADRE DE ELEMENTOS PAGINADOS
    getNodoPadreElementosPaginados(){
        return this.l_nodoPadreElementosPaginados;
    }
    setNodoPadreElementosPaginados(p_nodoPadreElementosPaginados){
        if( _validaNoNullNoUndefined(p_nodoPadreElementosPaginados) ){
            // actualiza valor de su propiedad
            this.l_nodoPadreElementosPaginados = p_nodoPadreElementosPaginados;
            // dibuja los elementos y el paginador
            this.dibuja();
        }
    }

    //GET y SET  POSICION EN EL NODO PADRE DE ELEMENTOS PAGINADOS
    getPosicionEnNodoPadreElementosPaginados(){
        return this.l_posicionEnNodoPadreElementosPaginados;
    }
    setPosicionEnNodoPadreElementosPaginados(p_posicionEnNodoPadreElementosPaginados){
        if( _validaNoNullNoUndefined(p_posicionEnNodoPadreElementosPaginados) && _validaCadenaNoVacia(p_posicionEnNodoPadreElementosPaginados) && _validaPosicionInsertAdjacentHTML(p_posicionEnNodoPadreElementosPaginados) ){
            // actualiza valor de su propiedad
            this.l_posicionEnNodoPadreElementosPaginados = p_posicionEnNodoPadreElementosPaginados;
            // dibuja los elementos y el paginador
            this.dibuja();
        }
    }

    //GET y SET NUMERO DE PAGINA ACTUAL
    getNumPagActual(){
        return this.l_paginaActual;
    }
    setNumPagActual(p_paginaActual){
        if( _validaNoNullNoUndefined(p_paginaActual) && _validaNumero(p_paginaActual) ){
            // actualiza valor de su propiedad
            this.l_paginaActual = p_paginaActual;
            if( _validaNoNullNoUndefined(this.getNumPagTotales()) && _validaNoNullNoUndefined(this.getArrayTotal()) && _validaNoNullNoUndefined(this.getNumElementosXPagina()) ){
                // actualiza la plantilla del paginador
                this.privateUpdatePlantillaPaginadorHTML(p_paginaActual,this.getNumPagTotales());
                // actualiza el array tramo
                this.setArrayTramo(this.getArrayTotal(),this.getNumElementosXPagina(),this.getNumPagActual());
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    //GET y SET  NUMERO DE PAGINAS TOTALES
    getNumPagTotales(){
        return this.l_numPagTotales;
    }
    setNumPagTotales(p_numTotalPag){
        if( _validaNoNullNoUndefined(p_numTotalPag) && _validaNumero(p_numTotalPag) ){
            // actualiza valor de su propiedad
            this.l_numPagTotales = p_numTotalPag;
            if( _validaNoNullNoUndefined(this.getNumPagActual()) ){
                // actualiza la plantilla HTML del paginador
                this.privateUpdatePlantillaPaginadorHTML(this.getNumPagActual(),p_numTotalPag);
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    // GET y SET NUMERO DE ELEMENTOS POR PAGINA
    getNumElementosXPagina(){
        return this.l_numElementosXPag;
    }
    setNumElementosXPagina(p_numElementosXPag){
        if( _validaNoNullNoUndefined(p_numElementosXPag) && _validaNumero(p_numElementosXPag) && _validaNumeroMayorQueCero(p_numElementosXPag) ){
            // actualiza valor de su propiedad
            this.l_numElementosXPag = p_numElementosXPag;
            // actualiza pagina actual
            this.setNumPagActual(1);
            // comprueba array no nullo ni undefined
            if( _validaNoNullNoUndefined(this.getArrayTotal()) ){
                // actualizamos el numero de paginas totales que depende de la longitud del array y del numero de elementos por pagina
                this.setNumPagTotales(Math.ceil(this.getArrayTotal().length/this.getNumElementosXPagina()));
                // actualiza array tramo
                this.setArrayTramo(this.getArrayTotal(),this.getNumElementosXPagina(),this.getNumPagActual());
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    // GET y SET ARRAY TOTAL
    getArrayTotal(){
        return this.l_arrayTotal;
    }
    setArrayTotal(p_arrayTotal){
        if( _validaNoNullNoUndefined(p_arrayTotal) ){
            // actualiza valor de su propiedad
            this.l_arrayTotal = p_arrayTotal;
            //comprueba metodo get
            if( _validaNoNullNoUndefined(this.getNumElementosXPagina()) ){
                // actualizamos el numero de paginas totales que depende de la longitud del array y del numero de elementos por pagina
                this.setNumPagTotales(Math.ceil(p_arrayTotal.length/this.getNumElementosXPagina()));
                this.setArrayTramo(this.getArrayTotal(),this.getNumElementosXPagina(),this.getNumPagActual());
                // dibuja los elementos y el paginador
                this.dibuja();
            }
        }
    }

    // GET y SET ARRAY TOTAL
    getArrayTramo(){
        return this.l_arrayTramo;
    }
    setArrayTramo(p_arrayTotal,p_numElementosXPag,p_numPagSolicitada){

        if( _validaNoNullNoUndefined(p_arrayTotal) && _validaNoNullNoUndefined(p_numElementosXPag) && _validaNoNullNoUndefined(p_numPagSolicitada) && _validaNoNullNoUndefined(this.getNumPagTotales()) && _validaNumeroMayorQueCero(p_numPagSolicitada) && _validaNumeroMayorQue(this.getNumPagTotales(),p_numPagSolicitada) && _validaNumeroMayorQueCero(p_numElementosXPag) ){
            // actualiza valor de su propiedad
            this.l_arrayTramo = this.privateCalculaArrayTramo(p_arrayTotal,p_numElementosXPag,p_numPagSolicitada);
            // dibuja los elementos y el paginador
            this.dibuja();
        }
    }

    //GET PLANTILLA HTML DEL PAGINADOR
    getPlantillaPaginadorHTML(){
        return this.l_plantillaPaginadorHTML;
    }

    /* ------------------ METODOS UTILES ------------------ */

    // METODO PRIVADO: ACTUALIZA la propiedad "l_plantillaPaginadorHTML" con el numero de paginas totales y numero de pagina actual pasadas por parametro
    privateUpdatePlantillaPaginadorHTML(p_numPagActual,p_numPagTotal){
        if( _validaNoNullNoUndefined(p_numPagActual) && _validaNoNullNoUndefined(p_numPagTotal) && _validaNumero(p_numPagActual) && _validaNumero(p_numPagTotal) ){
            // variable auxiliar que guarda el numero de pagina actual convertida a entero
            let l_numPagActualAux = Number.parseInt(p_numPagActual);
            // variable auxiliar que guarda el numero de paginas totales convertida a entero
            let l_numPagTotalesAux = Number.parseInt(p_numPagTotal);

            // crea la plantilla del paginador HTML
            this.l_plantillaPaginadorHTML = this.privateCreatePlantillaPaginadorHTML(l_numPagActualAux,l_numPagTotalesAux);
        }
    }

    // METODO PRIVADO: Funcion que obtiene la plantilla HTML con varias paginas y el contenedor de la pagina
    privateCreatePlantillaPaginadorHTML(p_numPagActual,p_numPagTotal){

        let l_plantillaPagina =
        `
        <!-- grupo paginas -->
        <div datos="caja-paginas" class="mw-20rem d-flex justify-content-center align-items-center bg-grisSecondaryExtraSoft ${this.getBorder()} roundedRem-5 text-truncate p-2">

            <!-- pag Anterior -->
            <a href='javascript:void(0)' pagina='pagina-anterior' class='${this.getTextColor()} ${this.getTamanyoFuente()} mr-1'>
              <i class='fas fa-arrow-alt-circle-left'></i>
            </a>
        `
        ;
       
        for(let i = 1; i <= p_numPagTotal;i++){
            if(i == p_numPagActual){
                l_plantillaPagina += 
                `
                    <a href='javascript:void(0)' pagina='${i}' class='mx-1 ${this.getTamanyoFuente()} ${this.getTextColor()} ${this.getCssClassActiveNumPag()}'>
                        ${i}
                    </a>
                `;
            }else{
                l_plantillaPagina += 
                `
                    <a href='javascript:void(0)' pagina='${i}' class='mx-1 ${this.getTamanyoFuente()} ${this.getTextColor()} '>
                        ${i}
                    </a>
                `;
            }
        }

        l_plantillaPagina+= 
        `
            <!-- pag Siguiente -->
            <a href='javascript:void(0)' pagina='pagina-siguiente' class='${this.getTextColor()} ${this.getTamanyoFuente()} ml-1'>
              <i class='fas fa-arrow-alt-circle-right'></i>
            </a>
        </div>
        `;

        // console.log(l_plantillaPaginador);
        return (l_plantillaPagina);

    }

    // EVENTO 'CLICK' EN EL NUMERO DE PAGINA
    clickNumPagina(){
        // click en paginas 1..2..3..
        this.getNodoPadrePaginador().addEventListener('click',(p_evento) =>
        {
            // variable guarda el numero de pagina pulsado en formato caracter
            let l_numPulsadoChar = p_evento.target.getAttribute("pagina");
            // variable guarda el numero de pagina convertido a numero
            let l_numPulsado = Number.parseInt(l_numPulsadoChar);
            // si se pulsa pagina concreta
            if(p_evento.target.nodeName == "A"){
                if(_validaNumero(l_numPulsado)){
                    // actualizamos el valor de pagina actual con el valor de la pagina pulsada
                    this.setNumPagActual(l_numPulsado);
                    // dibuja los elementos y el paginador
                    this.dibuja();
                }
            // si se pulsa el icono de 'previous' o 'next' pagina
            }else if(p_evento.target.nodeName == "I"){
                l_numPulsadoChar = p_evento.target.parentNode.getAttribute("pagina");
                if(l_numPulsadoChar == 'pagina-anterior'){
                    // Si nos encontramos en una pagina mayor a la 1
                    if(this.getNumPagActual() > 1){
                        // actualizamos el valor de pagina actual a una pagina menos a la actual
                        this.setNumPagActual(this.getNumPagActual()-1);
                        // dibuja los elementos y el paginador
                        this.dibuja();
                    }
                }else if(l_numPulsadoChar == 'pagina-siguiente'){
                    if(this.getNumPagActual() < this.getNumPagTotales()){
                        // actualizamos el valor de pagina actual a una pagina mas a la actual
                        this.setNumPagActual(this.getNumPagActual() + 1);
                        // dibuja los elementos y el paginador
                        this.dibuja();
                    }
                }
            }
        });
    }


    // METODO PRIVADO: Funcion que calcula y devuelve el tramo de array seleccionado
    privateCalculaArrayTramo(p_arrayTotal,p_numElementosXPag,p_numPagSolicitada){
        if( _validaNoNullNoUndefined(p_arrayTotal) && _validaNoNullNoUndefined(p_numElementosXPag) && _validaNoNullNoUndefined(p_numPagSolicitada) && _validaArray(p_arrayTotal) && _validaNumeroMayorQueCero(p_numElementosXPag) && _validaNumeroMayorQueCero(p_numPagSolicitada) ){
            // variables para el problema
            let l_arrayAuxTramo = [];
            let l_indiceInicio;
            let l_indiceFin;
            // variables auxiliares
            let l_numElementosXPagAux = Number.parseInt(p_numElementosXPag);
            let l_numPagSolicitadaAux = Number.parseInt(p_numPagSolicitada);
            let l_numPagTotalesAux;
            // numero paginas totales
            l_numPagTotalesAux = Math.ceil(p_arrayTotal.length/l_numElementosXPagAux);

            if(l_numPagSolicitadaAux < 1){
                l_indiceInicio = 0;
                l_indiceFin = l_numElementosXPagAux-1;
            }else if(l_numPagSolicitadaAux >= l_numPagTotalesAux){
                l_indiceInicio = (l_numPagTotalesAux*l_numElementosXPagAux)-l_numElementosXPagAux;
                l_indiceFin = p_arrayTotal.length-1;
            }else{
                l_indiceInicio = (l_numPagSolicitadaAux*l_numElementosXPagAux)-l_numElementosXPagAux;
                l_indiceFin = (l_numPagSolicitadaAux*l_numElementosXPagAux)-1;
            }

            // Trozo de array que cumple el tramo
            l_arrayAuxTramo = p_arrayTotal.slice(l_indiceInicio,l_indiceFin+1);
            // this.setArrayElemTotales(l_arrayAuxTramo);
            
            return l_arrayAuxTramo;
        }
    }
    

    // Dibuja los elementos y el paginador
    dibuja(){

        if( _validaNoNullNoUndefined(this.getNodoPadreElementosPaginados()) && _validaNoNullNoUndefined(this.getNodoPadrePaginador()) && _validaNoNullNoUndefined(this.getArrayTramo()) && _validaNoNullNoUndefined(this.getPosicionEnNodoPadreElementosPaginados()) && _validaNoNullNoUndefined(this.getPlantillaPaginadorHTML()) && _validaNoNullNoUndefined(this.getPosicionEnNodoPadrePaginador())){

            // vacia el espacio donde van los productos
            _removeChildElementsInDom(this.getNodoPadreElementosPaginados());
            // vacia el espacio donde se encuentra el paginador
            _removeChildElementsInDom(this.getNodoPadrePaginador());

            // anyade los elementos a la pagina de elementos
            for(let l_item of this.getArrayTramo()){
                if(l_item != null){
                    _addElementInDom(this.getNodoPadreElementosPaginados(),_plantillaElementProdConsulta(l_item),this.getPosicionEnNodoPadreElementosPaginados());
                }
            }

            // anyade los numeros de paginas al paginador
            _addElementInDom(this.getNodoPadrePaginador(),this.getPlantillaPaginadorHTML(),this.getPosicionEnNodoPadrePaginador());
        }
    }


    // funcion toSTRING
    toString(){
        // console.log('TCL: Paginador -> toString -> l_paginaActual', this.getNumPagActual());
        // console.log('TCL: Paginador -> toString -> l_numPagTotales', this.getNumTotalPag());
        // console.log('TCL: Paginador -> toString -> l_border', this.getBorder());
        // console.log('TCL: Paginador -> toString -> l_textColor', this.getTextColor());
		// console.log('TCL: Paginador -> toString -> l_tamanyoFuente', this.getTamanyoFuente());
    }


    // //GET y SET l_arrayElemTotales
    // getArrayElemTotales(){
    //     return this.l_arrayElemTotales;
    // }

    // setArrayElemTotales(p_arrayIn){

    //     try {


    //         if(p_arrayIn.length == 0){
    //             throw ('Error, La variable pasada por parámetro, p_arrayIn, no puede ser vacía');
    //         }else{
    //             // this.l_arrayElemTotales = p_arrayIn.slice();
    //             this.l_arrayElemTotales = p_arrayIn;


    //             // Actualiza numero total de paginas
    //             this.setNumPagTotal(Math.ceil(l_arrayElemTotales.length/this.getElementosXPagina()));
    //         }
    //     } catch (error) {
	// 		console.log('TCL: setIndiceInicioPag -> }catch -> error:', error)
    //     }
    // }

    // //GET y SET l_elementosXPagina
    // getElementosXPagina(){
    //     return this.l_elementosXPagina;
    // }

    // setElementosXPagina(p_numElementosXPagina){
    //     try {
            
    //         let l_numEle = Number.parseInt(p_numElementosXPagina);
    //         if(Number.isNaN(l_numEle)){
    //             throw ('Error, La variable pasada por parámetro, p_numElementosXPagina, no es un número');
    //         }else{
    //             this.l_elementosXPagina = l_numEle;
    //         }
    //     } catch (error) {
	// 		console.log('TCL: setElementosXPagina -> }catch -> error:', error)
    //     }
    // }

    // //GET y SET l_indiceInicioPag
    // getIndiceInicioPag(){
    //     return this.l_indiceInicioPag;
    // }

    // setIndiceInicioPag(p_indiceInicio){

    //     try {
    //         let l_indiceNum = Number.parseInt(p_indiceInicio);
    //         if(Number.isNaN(l_indiceNum)){
    //             throw ('Error, La variable pasada por parámetro, p_indiceInicio, no es un número');
    //         }else{
    //             this.l_indiceInicioPag = l_indiceNum;
    //         }
    //     } catch (error) {
	// 		console.log('TCL: setIndiceInicioPag -> }catch -> error:', error)
    //     }
    // }

    // //GET y SET l_indiceFinPag
    // getIndiceFinPag(){
    //     return this.l_indiceFinPag;
    // }

    // setIndiceFinPag(p_indiceFin){

    //     try {
    //         let l_indiceNum = Number.parseInt(p_indiceFin);
    //         if(Number.isNaN(l_indiceNum)){
    //             throw ('Error, La variable pasada por parámetro, p_indiceFin, no es un número');
    //         }else{
    //             this.l_indiceFinPag = l_indiceNum;
    //         }
    //     } catch (error) {
	// 		console.log('TCL: setIndiceFinPag -> }catch -> error:', error)
    //     }
    // }


    // // Funcion ACTUALIZA paginador
    // redibujaPaginador(p_numPagTotal=1,p_numPagActual=1){
    //     _removeChildElementsInDom(this.getNodoPadre());
    //     this.updatePlantillaPaginadorHTML(p_numPagTotal,p_numPagActual);
    //     _addElementInDom(this.getNodoPadre(),this.getPlantillaPaginadorHTML(),this.getPosicionEnNodoPadre());
    //     // Actualiza pagina actual y colorea el numero de pagina
    //     // this.setNumPagActual(p_numPagActual);
    // }


    // // Funcion ACTUALIZA paginador
    // redibujaPaginador(){
    //     _removeChildElementsInDom(this.getNodoPadrePaginador());
    //     // this.updatePlantillaPaginadorHTML(p_numPagTotal,p_numPagActual);
    //     _addElementInDom(this.getNodoPadrePaginador(),this.getPlantillaPaginadorHTML(),this.getPosicionEnNodoPadrePaginador());
    //     // Actualiza pagina actual y colorea el numero de pagina
    //     // this.setNumPagActual(p_numPagActual);
    // }


    // // FUNCION ANYADE PRODUCTOS A UNA PAGINA CONCRETA
    // privateAnyadeElementosAPagina(p_arrayTramoADibujar){

    //     // calcula numero de paginas total segun el array de busqueda filtrado y el numero de productos mostrados por pagina
    //     // let l_numPagTotal = Math.ceil(p_arrayProdFiltrados.length/p_selectNumProdMostradosXPaginaValue);

    //     // calcula y devuelve el array con el tramo a mostrar en la pagina
    //     // let l_arrayProdXPag = this.calculaArrayTramo(p_arrayParaPaginaDeterminada,p_numPagSeleccionada,this.getNumTotalPag(),p_selectNumProdMostradosXPaginaValue);

    //     for(let l_item of p_arrayTramoADibujar){
    //         if(l_item != null){
    //             // _addElementInDom(p_nodoPadre,_plantillaElementProdConsulta(l_p),c_BEFOREEND);
    //             _addElementInDom(this.getNodoPadreElementosPaginados(),_plantillaElementProdConsulta(l_item),this.getPosicionEnNodoPadreElementosPaginados());
    //         }
    //     }

    //     //PIE PAGINA
    //     // recalcula numero total paginas para redibujarPaginador
    //     // this.redibujaPaginador();
        
    //     // EVENTO 'CLICK' EN EL NUMERO DE PAGINA
    //     // this.clickNumPagina(p_arrayParaPaginaDeterminada,p_nodoPadre,p_selectNumProdMostradosXPaginaValue);

        
    // }







}


