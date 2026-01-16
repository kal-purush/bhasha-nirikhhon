'use strict';

(function () {
  var jogadorAtivo = 'j1';
  var selectedPhago;
  var connection;
  var game;

  $(window).load(() => {
    inicializarWS();

    $.get('/api/game').done((data) => {
      game = data;
      console.log(game);

      initializePanel();
      initializePhagoFactory();
    });
  });

  function inicializarWS() {
    connection = new WebSocket(websocketHost())
    connection.onopen = function () {
      console.log('Websocket connected');
    }
    connection.onmessage = function (e) {
      var payload = JSON.parse(e.data);
      console.log('Received payload', payload);
    }

    function websocketHost() {
      if(window.location.protocol === 'https:') {
        return 'wss://' + window.location.host;
      } else {
        return 'ws://' + window.location.host;
      }
    }
  }

  function initializePanel() {
    for (var i = 1; i <= (game.sideSize * game.sideSize); i++) {
      $('#painel').append('<li class="noselect" id="q' + i + '"></li>');
    }
    $('#painel').css('width', (game.sideSize * 2) + 'em');

    $('#painel').selectable({
      delay: 150,
      stop: realizarJogada,
      selecting: calcularTamanhoRetangulo,
      unselecting: calcularTamanhoRetangulo,
      disabled: true
    });
  }

  function initializePhagoFactory() {
    $('.phagos').click((evento) => {
      var elemento = evento.target;
      if(elemento.parentNode.id === jogadorAtivo) {
        selectPhago(elemento);
      };
    });

    pushInitialPhagos('j1');
    pushInitialPhagos('j2');

    function pushInitialPhagos(jogador) {
      var phagos = game.initialPhagos[jogador];
      for(var i = 0; i < phagos.length; i++) {
        pushPhago(jogador, phagos[i]);
      }
    }
  }

  function pushPhago(jogador, phago) {
    var elemento = $('<li class="noselect">' + phago + '</li>');
    $('#' + jogador).append(elemento);
    elemento.slideDown();
  }

  function getRandomPhago(jogador) {
    $.get('/api/game/randomPhago').done((phago) => {
      pushPhago(jogador, phago);
    });
  }

  function calcularTamanhoRetangulo(evento) {
    var selecting = $('.ui-selecting');
    var pontosSelecionados = [];
    for(var i = 0; i < selecting.length; i++) {
      pontosSelecionados.push(extrairPonto(selecting[i]));
    }

    if(pontosSelecionados.length !== 0) {
      mostrarConta(pontosSelecionados);
    }
  }

  function extrairPosicao(quadrado) {
    return Number.parseInt(quadrado.id.substring(1));
  }

  function extrairPonto(quadrado) {
    var posicao = extrairPosicao(quadrado) - 1;
    var x = (posicao % game.sideSize) + 1;
    var y = Math.floor(posicao / game.sideSize) + 1;
    return {x: x, y: y};
  }

  function cantoDireitoAbaixo(figura) {
    return figura.reduce((canto, ponto) => {
      return {
        x: canto.x > ponto.x ? canto.x : ponto.x,
        y: canto.y > ponto.y ? canto.y : ponto.y
      };
    }, {x: 0, y: 0});
  }

  function cantoEsquerdoAcima(figura) {
    return figura.reduce((canto, ponto) => {
      return {
        x: canto.x < ponto.x ? canto.x : ponto.x,
        y: canto.y < ponto.y ? canto.y : ponto.y
      };
    }, {x: Number.POSITIVE_INFINITY, y: Number.POSITIVE_INFINITY});
  }

  function mostrarConta(pontosSelecionados) {
    var menor = cantoEsquerdoAcima(pontosSelecionados);
    var maior = cantoDireitoAbaixo(pontosSelecionados);

    var x = maior.x - menor.x + 1;
    var y = maior.y - menor.y + 1;
    $('#conta').text(x + " x " + y + " = " + x * y);
    $('#conta').show();
  }

  function selectPhago(phago) {
    selectedPhago = phago;
    $('.phagos li').removeClass('selecionado');
    $(phago).addClass('selecionado');
    $('#painel').selectable('enable');
  }

  function realizarJogada(event) {
    if(!selectedPhago) {
      return;
    }

    $('#conta').fadeOut(2000);
    var quadradosSelecionados = $('.ui-selected');
    var produtoSelecionado = $(selectedPhago).text();
    if(!(produtoSelecionado == quadradosSelecionados.size())
      || quadradosSelecionados.hasClass('ocupado')) {
      $('#painel li').removeClass('ui-selected');
      return;
    };

    $(selectedPhago).slideUp();
    getRandomPhago(jogadorAtivo);

    $('#painel li').addClass('color-transition');

    var selecionados = $('#painel .ui-selected');

    selecionados.addClass('ocupado');
    selecionados.addClass('ocupado' + jogadorAtivo);

    var pontoInicial = extrairPonto(selecionados[0]);

    var grid = montarGrid();
    fagocitar(jogadorAtivo, grid, pontoInicial);
    contarPontos(grid);

    jogadorAtivo = jogadorAtivo === 'j1' ? 'j2' : 'j1';
    limparJogada();
  }

  function contarPontos(grid) {
    var placarJ1 = 0;
    var placarJ2 = 0;

    for (var y = 0; y < game.sideSize + 1; y++) {
      for (var x = 0; x < game.sideSize + 1; x++) {
        if(grid[y][x].jogador === "j1") {
          placarJ1++;
        } else if(grid[y][x].jogador === "j2") {
          placarJ2++;
        }
      }
    }

    $('#placar-j1').text(placarJ1);
    $('#placar-j2').text(placarJ2);
  }

  function fagocitar(jogador, grid, pontoInicial) {
    function pontosAoRedor(ponto) {
      var pontos = [];

      if(ponto.x > 1) {
        pontos.push({x: ponto.x - 1, y: ponto.y});
      }
      if(ponto.x < game.sideSize) {
        pontos.push({x: ponto.x + 1, y: ponto.y});
      }
      if(ponto.y > 1) {
        pontos.push({x: ponto.x, y: ponto.y - 1});
      }
      if(ponto.y < game.sideSize) {
        pontos.push({x: ponto.x, y: ponto.y + 1});
      }
      return pontos;
    }

    Array.prototype.pIndexOf = function(p) {
      for (var i = 0; i < this.length; i++) {
        if (this[i].x == p.x && this[i].y == p.y) {
          return i;
        }
      }
      return -1;
    }

    var figura = [pontoInicial];

    for (var i = 0; i < figura.length; i++) {
      var ponto = figura[i];
      var proximosPontos = pontosAoRedor(ponto);

      proximosPontos.forEach((ponto) => {
        if(figura.pIndexOf(ponto) === -1 && grid[ponto.y][ponto.x].jogador === jogador) {
          figura.push(ponto);
        }
      });
    }

    var esquerdoAcima = cantoEsquerdoAcima(figura);
    var direitoAbaixo = cantoDireitoAbaixo(figura);

    var encontrouQuadradoCinza = false;
    for (var y = esquerdoAcima.y; y <= direitoAbaixo.y; y++) {
      for (var x = esquerdoAcima.x; x <= direitoAbaixo.x; x++) {
        if(!grid[y][x]) {
          encontrouQuadradoCinza = true;
          break;
        };
      }
      if(encontrouQuadradoCinza) {
        break;
      }
    }

    if(!encontrouQuadradoCinza) {
      for (var y = esquerdoAcima.y; y <= direitoAbaixo.y; y++) {
        for (var x = esquerdoAcima.x; x <= direitoAbaixo.x; x++) {
          grid[y][x].jogador = jogador; //inócuo
          $('#q' + grid[y][x].id).removeClass('ocupadoj1');
          $('#q' + grid[y][x].id).removeClass('ocupadoj2');
          $('#q' + grid[y][x].id).addClass('ocupado' + jogador);
        }
      }
    }
  }

  function montarGrid() {
    var grid = [];

    for (var y = 0; y < game.sideSize + 1; y++) {
      grid.push([]);
      for (var x = 0; x < game.sideSize + 1; x++) {
        grid[y][x] = "";
      }
    }

    var selecionados = $('#painel .ocupado');

    for (var i = 0; i < selecionados.length; i++) {
      var posicao = extrairPosicao(selecionados[i]);
      var ponto = extrairPonto(selecionados[i]);
      var jogador = $(selecionados[i]).hasClass("ocupadoj1") ? "j1" : "j2";
      grid[ponto.y][ponto.x] = {id: posicao, jogador: jogador};
    }

    return grid;
  }

  function limparJogada() {
    selectedPhago = undefined;
    $('#painel li').removeClass('ui-selected');
    $('#painel').selectable('disable');
    $('painel li').removeClass('color-transition');
  }
})();