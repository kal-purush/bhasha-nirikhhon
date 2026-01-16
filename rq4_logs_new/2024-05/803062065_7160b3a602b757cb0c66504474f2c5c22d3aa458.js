var menu = prompt('Sistema para Calcular as Áreas\n\n1. Calcular Área do Triângulo\n2. Calcular Área do Retângulo\n3. Calcular Volume do Cubo\n4. Calcular Área do Círculo\n5. Sair') 

switch (menu){ 

  case  '1':
      var base = prompt('Digite o valor da BASE do Triângulo')
      var altura = prompt('Digite o valor da ALTURA do Triângulo')
              if(altura <= 0 || base <= 0 ) {
                alert ('Valor invalido!\n Base ou Altura menor que 1') 
                }  else{
                 var area = base * altura / 2 
                 alert(`O valor da area do Triangulo é ${area}`)
                }
        location.reload()
      break; 
  case  '2':
      var base = prompt('Digite o valor da BASE do Retângulo')
      var altura = prompt('Digite o valor da ALTURA do Retângulo')
              if(altura <= 0 || base <= 0 ) {
                alert ('Valor invalido!\n Base ou Altura menor que 1')
                }  else{
                 var area = base * altura
                 alert(`O valor da area do Retângulo é ${area}`)
                }
        location.reload()
      break;
  case  '3':
      var aresta = prompt('Digite o valor de uma das Aresta do Cubo')
                  if( aresta <= 0 ) {
                alert ('Valor invalido!\n Aresta menor que 1')
                }  else{
                 var area = aresta ** 3
                 alert(`O volume do cubo é igual a ${area}`)
                }
        location.reload()
      break;
   case  '4' :
      var raio = prompt('Digite o valor do Raio do Círculo')
                  if( raio <= 0 ) {
                alert ('Valor invalido!\n Raio menor que 1')
                }  else{
                 var area = raio * raio * 3.14
                 alert(`O valor Área do Círculo é igual a ${area}`)
                }
        location.reload()
      break;
    case '5':
        alert('Obrigador por usar nosso sistema de calculos')
        break;
  default:
      alert('Opção escolhida invalida')
    
}