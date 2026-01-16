var bauDeTesouroIMG = new Image();
var navioInimigoIMG = new Image();
var erroIMG = new Image();
var espacoIMG = new Image();
var planoDeFundoIMG = new Image();
bauDeTesouroIMG.src = "https://freepngimg.com/thumb/treasure/1-2-treasure-png.png";
navioInimigoIMG.src = "https://www.nicepng.com/png/full/324-3241512_caravel-clipart-old-ship-navio-pirata-peter-pan.png";
erroIMG.src = "https://img2.gratispng.com/20180404/bqw/kisspng-computer-icons-error-download-clip-art-x-mark-5ac54ca17a5a95.5409712615228796495012.jpg";
espacoIMG.src = "https://3.bp.blogspot.com/-AWcGi_u5o7A/VwJQFKQK5ZI/AAAAAAAADTo/wNg7hIVdp08vqUSKLtjBTF7caXAFDRP4Q/s200/quadrado-contorno-da-forma_318-50117.jpg";
planoDeFundoIMG.src = "https://1.bp.blogspot.com/-FjLJmT4funI/YVJTAlACSmI/AAAAAAAABMc/Fk3YRq9KVBIXVxpZvLl6xGQSfvP6Z7cbgCLcBGAsYHQ/s320/planoDeFundo.PNG";
var telaDoJogo = document.createElement("canvas");
telaDoJogo.width = 480;
telaDoJogo.height = 480;
telaDoJogo.id = "canvas";

function criarJogador(_nome, _id){
    var jogador = {
        id : _id,
        nome : _nome,
        pontuacao : 0,
        municao : 5,
	naviosDestruidos : 0
    };

    return jogador;
}

function criarObjeto(_nome, _imagem, _municaoExtra, _pontosExtra){
    var objeto = {
        nome : _nome,
        imagem : _imagem,
        municaoExtra : _municaoExtra,
        pontosExtra : _pontosExtra,
    };

    return objeto;
}

function criarEspaco(_imagem, _objeto){
    var espaco = {
        imagem : _imagem,
        objeto : _objeto,
        tiroAcertado : false
    };

    return espaco;
}

const campoVazio = criarObjeto("CAMPO VAZIO", erroIMG, 0, 0);

const navioInimigo = criarObjeto("NAVIO INIMIGO", navioInimigoIMG, 1, 10);

const bauDeTesouro = criarObjeto("BAU DE TESOURO", bauDeTesouroIMG, 0, 30);

const campoDeBatalha = {
    espacos : [],
    tamanho : [4, 4],
    inicializar(){
        for(var i = 0; i < this.tamanho[0]/*Linhas*/; i++){//cria os espaços do campo de batalha
            var spco = [];
            for(var j = 0; j < this.tamanho[1]/*Colunas*/; j++){
                spco.push(criarEspaco(espacoIMG, campoVazio));
            }
            this.espacos.push(spco);

        }
    },
    distribuirObjetos(_objeto, _quantidade){
        var aleatorio = [], contador = 0;
        while(contador < _quantidade){
            aleatorio[0] = Math.floor(Math.random() * 4);
            aleatorio[1] = Math.floor(Math.random() * 4);
            if(campoDeBatalha.espacos[aleatorio[0]][aleatorio[1]].objeto.nome == "CAMPO VAZIO"){
                campoDeBatalha.espacos[aleatorio[0]][aleatorio[1]].objeto = _objeto;
                contador++;
            }
        }
    },
    reiniciar(){
        for(var i = 0; i < this.tamanho[0]/*Linhas*/; i++){//reinicia o campo de batalha com objetos vazios.
            for(var j = 0; j < this.tamanho[1]/*Colunas*/; j++){
                this.espacos[i][j].imagem = espacoIMG;
                this.espacos[i][j].objeto = campoVazio;
                this.espacos[i][j].tiroAcertado = false;
            }
        }
    },
    atualizarTelaDoCampo(){
        var contextoDaTela = telaDoJogo.getContext("2d");
        contextoDaTela.clearRect(0, 0, telaDoJogo.width, telaDoJogo.height);
        contextoDaTela.drawImage(planoDeFundoIMG, 0, 0, 480, 480);
        for(var i = 0; i < this.tamanho[0]; i++){
            for(var j = 0; j < this.tamanho[1]; j++){
                contextoDaTela.drawImage(this.espacos[i][j].imagem, 64 + j * 100, 64 + i * 100,  96, 96);
            }
        }
    }
};

const jogo = {
    jogadores : [],
	foraDoJogo : [],
	naviosDoObjetivo : 5,
    jogadorAtual : null,
    inicializar(_numeroDeJogadores){
        document.getElementById("tela").appendChild(telaDoJogo);
        campoDeBatalha.inicializar();
        for(var i = 1; i <= _numeroDeJogadores; i++){//cria os jogadores necessarios para o jogo
            this.jogadores.push(criarJogador("JOGADOR " + i, i - 1));
        }
        this.atualizar();
        this.inicializarOnda();
        this.jogadorAtual = this.jogadores[0];
        campoDeBatalha.atualizarTelaDoCampo();
        document.getElementById("log").innerHTML = this.jogadorAtual.nome + ' escolha as coordenadas para o tiro.';
        var controleDeTiro = 
			'Linha: <input type="radio" checked="true" name="linha" value="0">A</input>'+
			'<input type="radio" name="linha" value="1">B</input>'+
			'<input type="radio" name="linha" value="2">C</input>'+
			'<input type="radio" name="linha" value="3">D</input><br>'+
			'Coluna: <input type="radio" checked="true" name="coluna" value="0">1</input>'+
			'<input type="radio" name="coluna" value="1">2</input>'+
			'<input type="radio" name="coluna" value="2">3</input>'+
			'<input type="radio" name="coluna" value="3">4</input><br>'+
			'<input type="button" value="Atirar" onclick="jogo.executarTiro()"></input>';
        document.getElementById("controle").innerHTML = controleDeTiro;
    },
    atualizar(){
        var infoJogadores = "";
        this.jogadores.forEach(atualizarInfoDosJogadores);
        function atualizarInfoDosJogadores(_jogadores){
            infoJogadores += "<div>" + _jogadores.nome + "<br>Pontos: " + _jogadores.pontuacao + "<br>Municao: " + _jogadores.municao + "</div>";
        }
        document.getElementById("infoDosJogadores").innerHTML = infoJogadores;
        campoDeBatalha.atualizarTelaDoCampo();
    },
    inicializarOnda(){
        campoDeBatalha.distribuirObjetos(navioInimigo, this.naviosDoObjetivo);
        campoDeBatalha.distribuirObjetos(bauDeTesouro, 2);
        
    },
    executarTiro(_linha, _coluna){
        for(var i = 0; i < document.getElementsByName("linha").length; i++){//verificar quais dos radio buttons esta marcado para linha
            if(document.getElementsByName("linha")[i].checked){
                _linha = document.getElementsByName("linha")[i].value;
            }
        }
        for(var i = 0; i < document.getElementsByName("coluna").length; i++){//verificar quais dos radio buttons esta marcado para coluna
            if(document.getElementsByName("coluna")[i].checked){
                _coluna = document.getElementsByName("coluna")[i].value;
            }
        }
        if(!campoDeBatalha.espacos[_linha][_coluna].tiroAcertado){
            this.jogadorAtual.municao -= 1;
            var objetoAcertado = campoDeBatalha.espacos[_linha][_coluna];
            if(objetoAcertado.objeto.nome == "NAVIO INIMIGO"){
                this.jogadorAtual.naviosDestruidos++;
            }
            objetoAcertado.imagem = objetoAcertado.objeto.imagem;
            this.jogadorAtual.municao += objetoAcertado.objeto.municaoExtra;
            this.jogadorAtual.pontuacao += objetoAcertado.objeto.pontosExtra;
            var mensagemDeLog = "";
            mensagemDeLog += this.jogadorAtual.nome +" acertou " + objetoAcertado.objeto.nome + "!<br>";
            for(var i = 0, j = 1; i < this.jogadores.length; i++, j++){
                if(this.jogadorAtual.id + j < this.jogadores.length){
                    this.jogadorAtual = this.jogadores[this.jogadorAtual.id + j];
                    if(this.jogadorAtual.municao > 0){
                        i = this.jogadores.length;
                    }
                    else{
                        if(this.jogadorAtual.id + 1 < this.jogadores.length){
                            this.jogadorAtual = this.jogadores[this.jogadorAtual.id + 1];
                        }
                        else{
                            this.jogadorAtual = this.jogadores[0];
                        }
                    }
                }
                else{
                    this.jogadorAtual = this.jogadores[0];
                    j = -1;
                }
            }
            if(this.jogadorAtual.municao == 0){
                mensagemDeLog += "FIM DE JOGO!";
                document.getElementById("controle").innerHTML = '<input type="button" value="Recomecar jogo" onclick="location.reload()"></input>';
            }
            else{
                mensagemDeLog += this.jogadorAtual.nome + ' escolha as coordenadas para o tiro.';
            }
            document.getElementById("log").innerHTML = mensagemDeLog;
            campoDeBatalha.espacos[_linha][_coluna].tiroAcertado = true;
            var naviosDestruidos = 0;
            this.jogadores.forEach(conferirObjetivo);
            function conferirObjetivo(_jogadores){
                naviosDestruidos += _jogadores.naviosDestruidos;
            }
            if(naviosDestruidos == this.naviosDoObjetivo){
                campoDeBatalha.reiniciar();
                this.inicializarOnda();
                this.jogadores.forEach(prepararJogadoresNovaOnda);
                function prepararJogadoresNovaOnda(_jogadores){
                    if(_jogadores.municao > 0){
                        _jogadores.pontuacao += 20 * _jogadores.naviosDestruidos;
                        _jogadores.municao = 5;
                        _jogadores.naviosDestruidos = 0;
                    }
                }
            }
        }
        else{
            mensagemDeLog = this.jogadorAtual.nome + ", um tiro nessas coordenadas já foi feito! Escolha outras coordenadas.";
            document.getElementById("log").innerHTML = mensagemDeLog;
        }
        
        console.log(naviosDestruidos);
        this.atualizar();
    }
};