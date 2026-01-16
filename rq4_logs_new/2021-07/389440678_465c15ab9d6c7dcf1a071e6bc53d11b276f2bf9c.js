const divProduto = document.getElementsByClassName("produto");
let data= [];

for (let produto of divProduto) {
  produto.addEventListener("click", (e) => {
    let price = produto.children[1].innerText;
    let cod = produto.children[2].children[0].innerText;
    let description = produto.children[3].innerText;    

    data.push({
      price,
      cod,
      description,
    }); 
    
    convertData = JSON.stringify(data);
    localStorage.setItem("carrinho", convertData);
  });
}