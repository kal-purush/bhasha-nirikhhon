function calcularCirculo(){ // Círculo: Praticar o uso da constante π (pi) e a potenciação.
    var raio = parseFloat(document.getElementById('raio').value);
    
    var area = Math.PI * Math.pow(raio, 2);
    
    document.getElementById('resultado').textContent = area.toFixed(2) + " m²";
}

function calcularTronco(){ // Tronco da pirâmide: é o sólido formado por uma secção transversal, paralela à base, com faces laterais trapezoidais e bases poligonais.
  // Obtenha os valores inseridos pelo usuário
  var raioMaior = parseFloat(prompt("Informe o valor do Raio maior"));
  var raioMenor = parseFloat(prompt("Informe o valor do Raio menor"));
  var altura = parseFloat(prompt("Informe o valor da altura"));

  // var area = Math.PI * (raioMaior + raioMenor) * (raioMaior - raioMenor + Math.sqrt(Math.pow(altura, 2) + Math.pow(raioMaior - raioMenor, 2)));

  var area = ((raioMaior+raioMenor)*altura/2)*4;

  console.log(area);

  // var r1 = parseFloat(prompt("Informe o valor do Raio maior"));
  // var r2 = parseFloat(prompt("Informe o valor do Raio menor"));
  // var h = parseFloat(prompt("Informe o valor da altura"));

  // var slantHeight = Math.sqrt(Math.pow(h, 2) + Math.pow((r2 - r1), 2));
  // var lateralArea = (r1 + r2) * slantHeight;

  // // Calcule a área das bases
  // var base1Area = Math.PI * Math.pow(r1, 2);
  // var base2Area = Math.PI * Math.pow(r2, 2);

  // // Calcule a área total
  // var totalArea = lateralArea + base1Area + base2Area;
  // console.log(totalArea);
}

function calcularRentagulo(){ // Retângulo: Semelhante ao quadrado, mas com lados diferentes.
  var base = parseFloat(prompt("Informe o valor da base"));
  var altura = parseFloat(prompt("Informe o valor da altura"));

  var area = base*altura;

  console.log(area);
}

function calcularCoroaDoCirculo(){ // Coroa do Círculo: Quando duas ou mais circunferências possuem o mesmo centro, são denominadas concêntricas. Nesse caso elas podem ter raio de tamanhos diferentes.

  var raioMaior = parseFloat(prompt("Informe o valor do raio maior"));
  var raioMenor = parseFloat(prompt("Informe o valor do raio menor"));

  var area = Math.PI * (Math.pow(raioMaior, 2) - Math.pow(raioMenor, 2));

  console.log(area);

}

function calcularTrapezoideIrregular(){ // Trapezoide Irregular: onde os quatro lados têm diferentes comprimentos. Teremos que aplicar fórmulas de geometria mais avançadas para chegar à área correta.

  var baseTrapezio = parseFloat(prompt("Informe o valor da base do trapézio"));
  var segundaBaseTrapezio = parseFloat(prompt("Informe o segundo valor da base do trapézio"));
  var altura = parseFloat(prompt("Informe o valor da altura"));

  var area = (baseTrapezio+segundaBaseTrapezio)/2*altura;

  console.log(area);

}

function calcularPoligono(){ // Polígono Regular com N Lados: Peça aos usuários que escolham um polígono regular com um número variável de lados (por exemplo, pentágono, hexágono, heptágono, etc.). Coloque pelo menos três opções.  Então usaremos fórmulas trigonométricas para dividir o polígono em triângulos e calcular a área total.

  var forma = parseInt(prompt("Digite 1 (pentágono), 2 (losango), 3 (quadrilátero)"));
  var area;

  if (forma === 1) {
    var lado = parseFloat(prompt("Informe o valor o comprimento de um dos lados do pentágono"));
    var apotemo = parseFloat(prompt("Informe o valor do apotamo"));

    area = lado * 5 * apotemo;
    
  }else if (forma === 2) {
    var diagonal1 = parseFloat(prompt("Informe o valor da primeira diagonal"));
    var diagonal2 = parseFloat(prompt("Informe o valor da segunda diagonal"));

    area = (diagonal1 * diagonal2) / 2;
  } else {
    var lados = parseFloat(prompt("Informe o valor do lado do quadrilátero"));

    area = Math.pow(lados, 2);
  }

  console.log(area);

}

function calcularQuadrado(){ // Quadrado: O cálculo da área de um quadrado é simples, bastando multiplicar o lado pelo lado.
 var lado1 = parseFloat(prompt("Informe o valor de um lado"));
 var lado2 = parseFloat(prompt("Informe o valor de outro lado"));

 var area = lado1*lado2;

 console.log(area);
}

function calcularCircular(){ // Setor Circular em um Círculo Aninhado: Vamos calcular a área de um setor circular dentro de um círculo maior. Isso envolve calcular a área do setor, subtrair a área do triângulo formado pelo raio do setor e depois adicionar a área do círculo maior.

}

document.getElementById("raio").addEventListener("keyup", function(event) {
    if (event.key === "Enter") {
      event.preventDefault();
      calculo();
    }
  });