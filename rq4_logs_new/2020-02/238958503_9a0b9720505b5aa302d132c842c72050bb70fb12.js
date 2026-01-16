function testeConclusao(perg2, perg3){
    var q2 = document.getElementById('perg2').value;
    var q3 = document.getElementById('perg3').value;
    
    if (q2 >= q3){
        alert ("Ano de conclusão menor ou igual ao de ingresso. Coloque um ano valido.");
        document.getElementById('perg3').value = "";
    }
}

function testeConclusao1(perg2, perg3){
    var q2 = document.getElementById('perg2').value;
    var q3 = document.getElementById('perg3').value;
    
    if (q3 != ""){
       if (q2 >= q3){
            alert ("Ano de conclusão menor ou igual ao de ingresso. Coloque um ano valido.");
            document.getElementById('perg3').value = "";
        } 
    }
    
}

function abreEstado(){
    var q6 = document.getElementById('perg6').value;
    if (q6 == "BR"){
        var q7 = document.getElementById('estado7');
        q7.classList.remove("estado");
        var p7 = document.getElementById('perg7');
        p7.setAttribute("required", "required");
    }else{
        var q7 = document.getElementById('estado7');
        q7.classList.add("estado");
        var p7 = document.getElementById('perg7');
        if (p7.hasAttribute("required")){
            p7.removeAttribute("required");
        }
    }
}

function abre8(){
    var q8 = document.getElementsByName('perg8');
    if (q8[0].checked){
        var q9 = document.getElementById('nove');
        q9.classList.remove("esconde");
        var p9 = document.getElementById('perg9');
        p9.setAttribute("required", "required");
        var p10 = document.getElementById('perg10');
        p10.setAttribute("required", "required");
    }else if (q8[1].checked){
        var q9 = document.getElementById('nove');
        q9.classList.add("esconde");
        var p9 = document.getElementById('perg9');
        if (p9.hasAttribute("required")){
            p9.removeAttribute("required");
        }     
        var p10 = document.getElementById('perg10');
        if (p10.hasAttribute("required")){
            p10.removeAttribute("required");
        }
    }
}

function abre9(){
    var q9 = document.getElementsByName('perg9');
    if (q9[0].checked || q9[1].checked){
        var q10 = document.getElementById('dez');
        q10.classList.remove("esconde1");
        var p10 = document.getElementById('perg10');
        p10.setAttribute("required", "required");
    }else if (q9[2].checked){
        var q10 = document.getElementById('dez');
        q10.classList.add("esconde1");
        var p10 = document.getElementById('perg10');
        if (p10.hasAttribute("required")){
            p10.removeAttribute("required");
        }         
    }
}

function abre20(){
    var q20 = document.getElementsByName('perg20');
    if (q20[0].checked || q20[1].checked){
        var q21 = document.getElementById('vinteum');
        q21.classList.remove("esconde");
        var p21 = document.getElementById('perg21');
        p21.setAttribute("required", "required");
    }else if (q20[2].checked){
        var q21 = document.getElementById('vinteum');
        q21.classList.add("esconde");
        var p21 = document.getElementById('perg21');
        if (p21.hasAttribute("required")){
            p21.removeAttribute("required");
        }         
    }
}

function abre28(){
    var q28 = document.getElementsByName('perg28');
    if (q28[0].checked){
        var q29 = document.getElementById('vintenove');
        q29.classList.remove("esconde");
        var p29 = document.getElementById('perg29');
        p29.setAttribute("required", "required");
    }else if (q28[1].checked){
        var q29 = document.getElementById('vintenove');
        q29.classList.add("esconde");
        var p29 = document.getElementById('perg29');
        if (p29.hasAttribute("required")){
            p29.removeAttribute("required");
        }         
    }
}

function abre30(){
    var q30 = document.getElementsByName('perg30');
    if (q30[0].checked){
        var q31 = document.getElementById('trintaeum');
        q31.classList.remove("esconde");
        var p31 = document.getElementById('perg31');
        p31.setAttribute("required", "required");
    }else if (q30[1].checked){
        var q31 = document.getElementById('trintaeum');
        q31.classList.add("esconde");
        var p31 = document.getElementById('perg31');
        if (p31.hasAttribute("required")){
            p31.removeAttribute("required");
        }         
    }
}

function abre32(){
    var q32 = document.getElementsByName('perg32');
    if (q32[0].checked){
        var q33 = document.getElementById('trintaetres');
        q33.classList.remove("esconde");
        var q44 = document.getElementById('quarentaequatro');
        q44.classList.add("esconde1");
        var p33 = document.getElementById('perg33');
        p33.setAttribute("required", "required");
        var p34 = document.getElementById('perg34');
        p34.setAttribute("required", "required");
        var p35 = document.getElementById('perg35');
        p35.setAttribute("required", "required");
        var p44 = document.getElementById('perg44');
        if (p44.hasAttribute("required")){
            p44.removeAttribute("required");
        }
    }else if (q32[1].checked){
        var q33 = document.getElementById('trintaetres');
        q33.classList.add("esconde");
        var q44 = document.getElementById('quarentaequatro');
        q44.classList.remove("esconde1");
        var p33 = document.getElementById('perg33');
        if (p33.hasAttribute("required")){
            p33.removeAttribute("required");
        }
        var p34 = document.getElementById('perg34');
        if (p34.hasAttribute("required")){
            p34.removeAttribute("required");
        }
        var p35 = document.getElementById('perg35');
        if (p35.hasAttribute("required")){
            p35.removeAttribute("required");
        }
        var p44 = document.getElementById('perg44');
        p44.setAttribute("required", "required");            
    }
}

function abre35(){
    var q35 = document.getElementsByName('perg35');
    if (q35[0].checked){
        var q36 = document.getElementById('trintaeseis');
        q36.classList.remove("esconde");
        var q44 = document.getElementById('quarentaequatro');
        q44.classList.add("esconde1");
        var p36 = document.getElementById('perg36');
        p36.setAttribute("required", "required");
        var p37 = document.getElementById('perg37');
        p37.setAttribute("required", "required");
        var p38 = document.getElementById('perg38');
        p38.setAttribute("required", "required");
        var p44 = document.getElementById('perg44');
        if (p44.hasAttribute("required")){
            p44.removeAttribute("required");
        }
    }else if (q35[1].checked){
        var q36 = document.getElementById('trintaeseis');
        q36.classList.add("esconde");
        var q44 = document.getElementById('quarentaequatro');
        q44.classList.remove("esconde1");
        var p36 = document.getElementById('perg36');
        if (p36.hasAttribute("required")){
            p36.removeAttribute("required");
        }
        var p37 = document.getElementById('perg37');
        if (p37.hasAttribute("required")){
            p37.removeAttribute("required");
        }
        var p38 = document.getElementById('perg38');
        if (p38.hasAttribute("required")){
            p38.removeAttribute("required");
        }
        var p44 = document.getElementById('perg44');
        p44.setAttribute("required", "required");            
    }
}

function abre38(){
    var q38 = document.getElementsByName('perg38');
    if (q38[0].checked){
        var q39 = document.getElementById('trintaenove');
        q39.classList.remove("esconde");
        var q43 = document.getElementById('quarentaetres');
        q43.classList.add("esconde1");
        var p39 = document.getElementById('perg39');
        p39.setAttribute("required", "required");
        var p40 = document.getElementById('perg40');
        p40.setAttribute("required", "required");
        var p41 = document.getElementById('perg41');
        p41.setAttribute("required", "required");
        var p42 = document.getElementById('perg42');
        p42.setAttribute("required", "required");
        var p43 = document.getElementById('perg43');
        if (p43.hasAttribute("required")){
            p43.removeAttribute("required");
        }
    }else if (q38[1].checked){
        var q39 = document.getElementById('trintaenove');
        q39.classList.add("esconde");
        var q43 = document.getElementById('quarentaetres');
        q43.classList.remove("esconde1");
        var p39 = document.getElementById('perg39');
        if (p39.hasAttribute("required")){
            p39.removeAttribute("required");
        }
        var p40 = document.getElementById('perg40');
        if (p40.hasAttribute("required")){
            p40.removeAttribute("required");
        }
        var p41 = document.getElementById('perg41');
        if (p41.hasAttribute("required")){
            p41.removeAttribute("required");
        }
        var p42 = document.getElementById('perg42');
        if (p42.hasAttribute("required")){
            p42.removeAttribute("required");
        }
        var p43 = document.getElementById('perg43');
        p43.setAttribute("required", "required");            
    }
}

function abre56(){
    var q56 = document.getElementsByName('perg56');
    if (q56[0].checked){
        var q57 = document.getElementById('cinquentaesete');
        q57.classList.remove("esconde");
    }else if (q56[1].checked){
        var q57 = document.getElementById('cinquentaesete');
        q57.classList.add("esconde");
    }
}

function abre56(){
    var q56 = document.getElementsByName('perg56');
    if (q56[0].checked){
        var q57 = document.getElementById('cinquentaesete');
        q57.classList.remove("esconde");
    }else if (q56[1].checked){
        var q57 = document.getElementById('cinquentaesete');
        q57.classList.add("esconde");
    }
}