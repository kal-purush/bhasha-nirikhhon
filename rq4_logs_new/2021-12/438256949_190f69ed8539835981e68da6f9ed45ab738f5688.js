const arrayDaTabela = [];
const tempoDoDia = 1000 * 3600 * 24;

const dadosFornecidos = () => ({

    cliente : document.getElementById("nomeDoCliente").value,
    vencimento : document.getElementById('dataDoVencimento').value,
    valor : parseFloat(document.getElementById('valorDaCompra').value),
    total: ""
});

function limpardados() {
    let limpar = document.getElementById("tabela2");
    limpar.innerHTML = "";
}

function adicionarNaPagina(item){

    let clienteTr = document.createElement("tr");

    let clienteTd = document.createElement("td");
    let vencimentoTd = document.createElement("td");
    let valorTd = document.createElement("td");
    let totaTd = document.createElement("td");

    clienteTd.textContent = item.cliente;
    vencimentoTd.textContent = item.vencimento;
    valorTd.textContent = item.valor;
    totaTd.textContent = item.total;

    clienteTr.appendChild(clienteTd);
    clienteTr.appendChild(vencimentoTd);
    clienteTr.appendChild(valorTd);
    clienteTr.appendChild(totaTd);

    let tabela = document.getElementById("tabela2");
    tabela.appendChild(clienteTr);
}

function inserirArray(){
   
    arrayDaTabela.push(dadosFornecidos());  
    limpardados();
    arrayDaTabela.forEach(adicionarNaPagina);

    document.getElementById("nomeDoCliente").value = "";
    document.getElementById('dataDoVencimento').value = "";
    document.getElementById('valorDaCompra').value = "";
}

function adiconarJuros(){

    limpardados();
    let jurosCalculado = arrayDaTabela.map(function(item, indice){

        const hoje = new Date();
        const vence = new Date(item.vencimento);

        let diferencaDeDias = (hoje.getTime() - vence.getTime()) / tempoDoDia;
        let taxa = (diferencaDeDias * 0.001) + 0.02;
        let valorCompra = item.valor;
        let totalPagar = "";
        
        if(diferencaDeDias > 0){
            totalPagar = (valorCompra + valorCompra * taxa).toFixed(2);
        }else{
            totalPagar = valorCompra
        }

        item.total = totalPagar;      
        adicionarNaPagina(item);

    });

    console.log(arrayDaTabela);
}

function adicionarNaPagina2(obj){

    let clienteTr = document.createElement("tr");

    let clienteTd = document.createElement("td");
    let vencimentoTd = document.createElement("td");
    let valorTd = document.createElement("td");
    let totaTd = document.createElement("td");

    clienteTd.textContent = obj.cliente;
    vencimentoTd.textContent = obj.vencimento;
    valorTd.textContent = obj.valor;
    totaTd.textContent = obj.total;

    clienteTr.appendChild(clienteTd);
    clienteTr.appendChild(vencimentoTd);
    clienteTr.appendChild(valorTd);
    clienteTr.appendChild(totaTd);

    let tabela = document.getElementById("tabela2");
    tabela.appendChild(clienteTr);
}


function organizarPorCliente (){
    const arrayDosClientes = [];

    function agruparPor(item, index) {
        return item.reduce(function (comparacao, objeto) {
          let valor = objeto[index];
          if (!comparacao[valor]) {
            comparacao[valor] = [];
          }
          comparacao[valor].push(objeto);
          return comparacao;
        }, {});
    }

      let grupodePessoas = agruparPor(arrayDaTabela, 'cliente');

      arrayDosClientes.push(grupodePessoas);

      var quadrados = arrayDosClientes.map(function(item){

          const teste = document.getElementById("teste");
          teste.innerHTML = item.valor;

        return item;
     });

     console.log(quadrados[0])     
}

function organizarPorVencimento (){
    const arrayDoVencimento = [];

    function agruparPor(item, index) {
        return item.reduce(function (comparacao, objeto) {
          let valor = objeto[index];
          if (!comparacao[valor]) {
            comparacao[valor] = [];
          }
          comparacao[valor].push(objeto);
          return comparacao;
        }, {});
    }

      let grupodeVencimento = agruparPor(arrayDaTabela, 'vencimento');

      arrayDoVencimento.push(grupodeVencimento);

      var quadrados = arrayDoVencimento.map(function(item){

          const teste = document.getElementById("teste");
          teste.innerHTML = item.valor;

        return item;
     });

     console.log(quadrados[0])     
}

//A UTILIZAÇÃO DO METODO REDUCE FUNCIONOU E OS DADOS INSERIDOS SÃO AGRUPADOS DE FORMA CERTA COMO SE PODE VER NO CONSOLE.LOG. ENTRETANTO,NÃO CONSEGUI FAZER OS DADOS APARECEREM NO HTML E, CONSEQUENTIMENTE, NA PÁGINA.