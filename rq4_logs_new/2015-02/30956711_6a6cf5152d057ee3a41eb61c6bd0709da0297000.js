var sId = '';
var sDescItem = '';
var dicImagenes = {};
var dicAyuda = {};
var dicItem = {};

var TipoInciSel = "";



function inicioPaginaTipoIncidencia() {

    limpiaVariablesInci();

    //cargo los iconos
    leeXMLIconos();

    //totalImg();  			//la primera vez informa esta var con el total de imagenes ...
    mostrarImagenes("");

}

function limpiaVariablesInci(){
    TipoInciSel='';
    sDireccionAlta = '';
    posAlta = '';
    sCoords='';
    sCoord_X='';
    sCoord_Y='';
    mapAlta = null;

    sId='';
    sDescItem='';

    //Comentario
    sComentario = '';

    sFoto = '';
    _inciAudioFichero='';

}


function leeXMLIconos() {
    // alert('leo xml');
    $.ajax({
        type: "GET",
        url: "tablas/iconosTemas.xml",
        dataType: "xml",
        success: function (xml) {
            $(xml).find('icoTema').each(function () {
                dicImagenes[$(this).find('id').text()] = "images/tipoInci/" + $(this).find('img').text();
                dicAyuda[$(this).find('id').text()] = $(this).find('desc').text();
                //guardem l'item del seleccionat
                dicItem[$(this).find('id').text()] = $(this).find('img').text().substr(0, $(this).find('img').text().indexOf("_"));
            });
        },
        error: function () {
            mensaje("Error processant arxiu XML","error");
        }, async: false
    });
}

function mostrarImagenes() {
    var sTagImg = "";
    var nInd = 0;
    var nIndVis = 0;
    for (sImagen in dicImagenes) {
        sTagImg += "<a href='' onclick='" + "selectTipo(" + sImagen + ")' data-mini='false' data-inline='false' data-role='button' data-theme='c' data-corners='true' data-shadow='true' data-iconshadow='true' data-wrapperels='span' class='ui-btn ui-shadow ui-btn-corner-all ui-fullsize ui-btn-block ui-first-child ui-btn-up-c'>"
        sTagImg += "<img alt='' src='" + dicImagenes[sImagen] + "' style='width:45px' />"
        sTagImg += "<div>" + dicAyuda[sImagen] + "</div>"
        sTagImg += "</a>"
    }
    $('#divTipoInci').html(sTagImg);

};

function selectTipo(p_tipo) {
    try{
        TipoInciSel = p_tipo;
        navigator.camera.getPicture(hacerfotoOK, hacerFotoERROR, { quality: 20, destinationType: Camera.DestinationType.DATA_URL, correctOrientation: true,sourceType:  Camera.PictureSourceType.CAMERA,  saveToPhotoAlbum: false });
        //abrirPagina('pageDatosIncidencia', false);
    }
    catch (ex){
        mensaje(ex.message);
        //abrirPagina('pageDatosIncidencia', false);
    }
}

function hacerfotoOK(imageData) {
    sFoto = imageData;
    abrirPagina('pageDatosIncidencia', false);
}
function hacerFotoERROR(mensaje) {
    sFoto = '';
    //abrirPagina('pageTipoIncidencia', false);
}

