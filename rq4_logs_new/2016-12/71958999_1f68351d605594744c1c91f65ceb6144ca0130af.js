var HTML = (function(){
    var button = function(titulo){
        if(typeof(titulo) !== "string")
            titulo = "boton";
        var boton = document.createElement("button");
        boton.textContent = titulo;
        return boton;
    };

    var img = function(src){
        if(typeof(src) !== "string")
            src = "";
        var imagen = document.createElement("img");
        imagen.setAttribute("src", src);
        return imagen;
    }

    var ul = function () {
        return document.createElement("ul");
    }

    var li = function(text){
        var elemento = document.createElement("li");
        if(typeof(text) === "string")                
            elemento.textContent = text;
        return elemento;
    }
    
    var input = function(){
        return document.createElement("input");
    }

    var div = function(){
        return document.createElement("div");
    }

    var label = function(titulo){
        if(typeof(titulo) !== "string")           
            titulo = "";
        var label = document.createElement("label");        
        label.textContent = titulo;        
        return label;
    }

    var tabla = function (_ths, _tds) {
        var nueva_tabla = document.createElement("table");
        var nuevo_thead = document.createElement("thead");
        nueva_tabla.appendChild(nuevo_thead);
        var nuevo_tr = document.createElement("tr");
        nuevo_thead.appendChild(nuevo_tr);
        //creacion de la cabecera de la tabla
        for(var i = 0; i < _ths.length; i++){
            var nuevo_th = document.createElement("th");
            nuevo_th.textContent = _ths[i];
            nuevo_tr.appendChild(nuevo_th);            
        }
        //creacion del cuerpo de la tabla
        var nuevo_body = document.createElement("tbody");
        nueva_tabla.appendChild(nuevo_body);
        for(var i = 0; i < _tds.length; i++){
            var nuevo_tr = document.createElement("tr");
            for (var j = 0; j < _tds[i].length; j++) {
                var nuevo_td = document.createElement("td");
		        nuevo_td.textContent=_tds[i][j];
		        nuevo_tr.appendChild(nuevo_td);               
            }
            nuevo_body.appendChild(nuevo_tr);
        }
        return nueva_tabla;
    };

    var select = function(_list){
        var select = document.createElement("select");
        for(var i = 0; i < _list.length; i++){
            var option = document.createElement("option");
            option.textContent = _list[i];
            select.appendChild(option);
        }
        return select;
    };
    
    return{
        "button":   button,
        "img":      img,
        "ul":       ul,
        "li":       li,
        "tabla":    tabla,
        "input":    input,
        "select":   select,
        "div":      div,
        "label":    label
    };
})();