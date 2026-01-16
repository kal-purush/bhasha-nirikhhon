let carrinho = JSON.parse(localStorage.getItem("carrinho")) || [];
const listaProdutos = document.getElementById("lista-produtos");
const listaCarrinho = document.getElementById("lista-carrinho");
const totalCarrinho = document.getElementById("total-carrinho");

function adicionarAoCarrinho(idProduto) {
  const produto = getDetalhesProduto(idProduto);

  if (produto) {
    carrinho.push(produto);
    salvarCarrinhoNoLocalStorage();
    recarregarPagina()
    atualizarCarrinho();
  }
}

function recarregarPagina() {
  window.location.reload();
  console.log('recarregando')
}




function removerProduto(index) {
  // const index = parseInt(idProduto)
  // console.log(index)
  carrinho.splice(index, 1);
  salvarCarrinhoNoLocalStorage();
  atualizarCarrinho();


}


function atualizarCarrinho() {
  listaCarrinho.innerHTML = "";
  let precoTotal = 0
  //index correponderá ao indice no array
  let index = 0;

  carrinho.forEach((item) => {
    const itemCarrinho = document.createElement("li");
    itemCarrinho.innerHTML = `${item.nome} - R$ ${item.preco.toFixed(2)} 
    <button onclick="removerProduto(${index})">Delete</button>`;
    listaCarrinho.appendChild(itemCarrinho);
    precoTotal += item.preco;
    index += 1
  });

  totalCarrinho.textContent = precoTotal.toFixed(2);
  GerarQRCode(precoTotal.toFixed(2))
}



function finalizarCompra() {
  // Aqui você pode implementar a lógica para processar a compra, enviar para o servidor, etc.
  alert("Compra finalizada! Total: R$ " + totalCarrinho.innerHTML);
  carrinho = []; // Limpar o carrinho após a compra
  atualizarCarrinho();
  salvarCarrinhoNoLocalStorage();
}


function getDetalhesProduto(idProduto) {
  const produtoSelecionado = document.querySelector(
    `[data-id="${idProduto}"]`
  );

  if (produtoSelecionado) {

    return {
      id: produtoSelecionado.dataset.id,
      nome: produtoSelecionado.dataset.nome,
      preco: parseFloat(produtoSelecionado.dataset.preco),
    };
  }

  return null;
}

function salvarCarrinhoNoLocalStorage() {
  localStorage.setItem("carrinho", JSON.stringify(carrinho));
}

window.onload = () => {
  atualizarCarrinho();
  // generateQRCode();
};

// segunda tentativa qrcode

function GerarQRCode(valorFinal) {
  // const precoFinal = document.getElementById('total-carrinho');
  // const valorFinal = parseFloat(precoFinal).value;
  // var inputUsuario = document.querySelector('textarea').value;
  var GoogleChartAPI = 'https://chart.googleapis.com/chart?cht=qr&chs=500x500&chld=H&chl=';
  var conteudoQRCode = GoogleChartAPI + valorFinal;
  document.querySelector('#QRCodeImage').src = conteudoQRCode;
}




  // const qrContainer = document.querySelector('#qr-code')
  // const qrPrice = document.querySelector('[data-preco]')
  
  
  // let colorLight = '#fff',
  //   colorDark = '#000',
  //   text = "www.ifms.edu.br",
  //   size = 300;
  
  
  // qrPrice.addEventListener('input', handleQRText)
  
  
  // function handleQRText(e) {
  //   // aqui dará erro (udefined)
  //   text = e.target.value;
  // }
  
  
  // function generateQRCode() {
  //   qrContainer.innerHTML = '';
  //   new QRCode("qr-code", {
  //     text,
  //     height: size,
  //     width: size,
  //     colorLight,
  //     colorDark,
  
  //   });
  // }