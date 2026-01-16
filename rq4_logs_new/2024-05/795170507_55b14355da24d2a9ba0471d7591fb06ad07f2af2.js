// Arrow function para verificar se o jogador está dentro da área segura
const estaNaAreaSegura = (posX, posY, areaSegura) => {
    return posX >= areaSegura.xMin &&
           posX <= areaSegura.xMax &&
           posY >= areaSegura.yMin &&
           posY <= areaSegura.yMax;
};

// Definindo os limites da área segura
const areaSegura = {
    xMin: 10,
    xMax: 50,
    yMin: 20,
    yMax: 60
};

// Coordenadas do jogador
const posicaoJogador = { x: 30, y: 40};

// Verificando se o jogador está dentro da área segura
const resultado = estaNaAreaSegura(posicaoJogador.x, posicaoJogador.y, areaSegura);
console.log( "O jogador está na área segura:", resultado ? "Sim" : "Não");