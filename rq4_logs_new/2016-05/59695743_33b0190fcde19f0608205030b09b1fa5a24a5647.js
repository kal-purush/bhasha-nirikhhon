
var MUITO_ABAIXO_DO_PESO = "Muito abaixo do peso";
var ABAIXO_DO_PESO = "Abaixo do peso";
var NO_PESO = "Saudável";
var ACIMA_DO_PESO = "Acima do peso";
var OBESIDADE = "Obesidade";
var OBESIDADE_SEVERA = "Obesidade severa";
var OBESIDADE_MORBIDA = "Obesidade mórbida";

function calcular(){
 calcImc(document.getElementById('peso').value , document.getElementById('altura').value , document.getElementById('dropdown-sexo').value );
}

function calcImc(peso, altura, sexo){
	// switch(sexo){
	// 	case "Masculino":
			return diagnosticoImcMasculino(calcMathImc(peso, altura));
		// case "Feminino":
		// 	return diagnosticoImcFeminino(calcMathImc(peso, altura));
		// default:
		// 	return "erro";
	// }
}

function calcMathImc(peso, altura){
	altura = altura/100;
	return peso/(Math.pow(altura,2));
}

function diagnosticoImcMasculino(imc){


        if(imc < 17){
            alert(MUITO_ABAIXO_DO_PESO);
        }else if(17 <= imc && imc <= 20.7){

            alert(ABAIXO_DO_PESO);
        }else if(20.8 <= imc && imc <= 26.4){

            alert(NO_PESO);
        }else if(26.5 <= imc && imc <= 31){

            alert(ACIMA_DO_PESO);
        }else if(31.1 <= imc && imc <= 36.4){

            alert(OBESIDADE);
        }
        else if(36.5 <= imc && imc <= 39.99){

            alert(OBESIDADE_SEVERA);
        }else{

            alert(OBESIDADE_MORBIDA);
        }
}

function diagnosticoImcFeminino(imc){


        if(imc < 17){
              alert(MUITO_ABAIXO_DO_PESO);
        }else if(17 <= imc && imc <= 19.1){

          alert(ABAIXO_DO_PESO);
        }else if(19.2 <= imc && imc <= 25.8){

          alert(NO_PESO);
        }else if(25.9 <= imc && imc <= 32.3){

            alert(ACIMA_DO_PESO);
        }else if(32.4 <= imc && imc <= 37.4){

          alert(OBESIDADE);
        }
        else if(37.5 <= imc && imc <= 39.99){

            alert(OBESIDADE_SEVERA);
        }else{

            alert(OBESIDADE_MORBIDA);
   		}
}