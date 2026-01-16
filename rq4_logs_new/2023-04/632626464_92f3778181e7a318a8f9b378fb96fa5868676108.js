import { LightningElement } from "lwc";
export default class App extends LightningElement {
 
 //criar variaveis e atributos.

  empresa = "Pokémon LTDA";
  cnpj = "333.555.741/0001";
  endereco = "Rua New York, 963";

  produto = "Caderno";
  descricao = "Caderno de capa dura";
  preco = "R$ 10,99";


oferta1 = "Na compra de 50 unidade, você ganha 5% de desconto."
oferta2 = "Comprando 30 caixas com 10 unidades, você ganha 3% de desconto."
		

  
  visible = true;
  visible1 = true; 
  
  trocarDiv(){
    this.visible = !this.visible;
    
  }
  trocarDiv1(){
   
    this.visible1 = !this.visible1;
  }
  
 

}