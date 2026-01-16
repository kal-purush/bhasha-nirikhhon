   function agregarPosteo(){
        var contenedor = documentElementById(contenedor-posteos)
    var elem_p = document.createElement('p');
    var elem_em = document.createElement('em');
    var elem_strong = document.createElement('strong');
    var txt_01 = document.createTextNode('Éste es un párrafo con una palabra ');
    var txt_02 = document.createTextNode('enfatizada');
    var txt_03 = document.createTextNode(' y otras aún ');
    var txt_04 = document.createTextNode('más enfatizadas');
    var txt_05 = document.createTextNode('.');
    contenedor.appendChild(elem_p)
    elem_em.appendChild(txt_02);
    elem_strong.appendChild(txt_04);
    elem_p.appendChild(txt_01);
    elem_p.appendChild(elem_em);
    elem_p.appendChild(txt_03);
    elem_p.appendChild(elem_strong);
    elem_p.appendChild(txt_05);
   }
    