/* If - Else e Swicth Case */

/* 1. Faça um Programa que peça dois números e imprima o maior deles.
*/

import java.util.Scanner;

public class Exercicio1 {
	
	public static void main (String[] args) {
		
		Scanner input = new Scanner(System.in);
		
		System.out.println("Digite dois números inteiros: ");
		int n1 = input.nextInt();
		int n2 = input.nextInt();
		
		if (n1 == n2) {
			System.out.println("Os números são iguais.");
		} else if (n1 > n2) {
			System.out.println("O primeiro número é maior que o segundo número.");
		} else {
			System.out.println("O segundo número é maior que o primeiro número.");
		
		}
	}
}

/* 2. Faça um Programa que peça um valor e mostre na tela se o valor é positivo ou
* negativo.
*/

import java.util.Scanner;

public class Exercicio2 {
	
	public static void main (String[]args) {
	
	Scanner input = new Scanner(System.in);
	
	System.out.println("Informe um número real, positivo ou nhegativo: ");
	float numero = input.nextFloat();
	
	if (numero > 0) {
		System.out.println("O número é positivo.");
		
	}else {
		System.out.println("O número é negativo.");
		
		}
	}
}

/* 3. Faça um Programa que verifique se uma letra digitada é "F" ou "M". Conforme a letra
* escrever: F - Feminino, M - Masculino, Sexo Inválido.
*/

import java.util.Scanner;

public class Exercicio3 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe seu sexo (F - Feminio / M - Maculino:");
		String sexo = input.nextLine();
		
		switch(sexo) {
		case "M", "m", "masulino", "Masculino", "MASCULINO": System.out.println("O sexo é Masculino.");{
		} break;
		case "F", "f", "feminino", "Femino", "FEMININO": System.out.println("O sexo é Femino.");{
		} break;
		default: System.out.println("Sexo Inválido");
		
		}
	}
}

/* 4. Faça um Programa que verifique se uma letra digitada é vogal ou consoante.
*/

import java.util.Scanner;

public class Exercicio4 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Digite uma vogal (a,e,i,o,u) ou consoante: ");
		String letra = input.nextLine();
		
		//usando if (letra.equalsIgnoreCase("a"))
		
		if (letra.length()>1) {
			System.out.println("Não é uma letra válida.");
		}else {
		switch (letra) {
		
		case "a", "A", "e", "E", "I", "i", "o", "O", "u", "U":{
			System.out.println("É uma vogal.");
		}break;
		default: System.out.println("É uma consoante.");
			
			}
		}
	}
}

/* 5. Faça um programa para a leitura de duas notas parciais de um aluno. O programa
* deve calcular a média alcançada por aluno e apresentar: o A mensagem "Aprovado",
* se a média alcançada for maior ou igual a sete; o A mensagem "Reprovado", se a
* média for menor do que sete; o A mensagem "Aprovado com Distinção", se a média
* for igual a dez.
*/

import java.util.Scanner;

public class Exercicio5 {

	public static void main(String[] args) {
		
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe a 1º nota (entre 0 e 10) e a 2º nota (entre 0 e 10): ");
		double n1 = input.nextDouble();
		double n2 = input.nextDouble();
		
		double media = (n1+n2)/2;
				
		if (media == 10) {
			System.out.println("Aprovado com distinção.");
		}else if (media >= 7 ) {
			System.out.println("Aprovado.");
		}else {
			System.out.println("Reprovado.");
		}
	}
}

/* 6. Faça um Programa que leia três números e mostre o maior deles.
*/

import java.util.Scanner;

public class Exercício6 {

	public static void main(String[] args) {
		
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe três números inteiros: ");
		int n1 = input.nextInt();
		int n2 = input.nextInt();
		int n3 = input.nextInt();
		
		if (n1 > n2 && n1 > n3) {
			System.out.println("O primeiro número é o maior: " + n1);
		} else if (n2 > n1 && n2 > n3) {
			System.out.println("O segundo número é o maior: " + n2);
		} else if (n3 > n1 && n3> n2) {
			System.out.println("O terceiro número é o maior: " + n3);
		} else if (n1 == n2 || n2 == n3 || n1 == n3){
			System.out.println("Entrada de dados com números iguais.");
		}
	}
}

/* 7. Faça um Programa que leia três números e mostre o maior e o menor deles.
*/

import java.util.Scanner;

public class Exercicio7 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe três números inteiros: ");
		int n1 = input.nextInt();
		int n2 = input.nextInt();
		int n3 = input.nextInt();
		
		if (n1 > n2 && n1 > n3) {
			System.out.println("O primeiro número é o maior: " + n1);
		} else if (n2 > n1 && n2 > n3) {
			System.out.println("O segundo número é o maior: " + n2);
		} else if (n3 > n1 && n3> n2) {
			System.out.println("O terceiro número é o maior: " + n3);
		}else {
			System.out.println("Entrada de dados com números iguais.");
		}
		
		if (n1 < n2 && n1 < n3) {
			System.out.println("O primeiro número é o menor: " + n1);
		} else if (n2 < n1 && n2 < n3) {
			System.out.println("O segundo número é o menor: " + n2);
		} else if (n3 < n1 && n3 < n2) {
			System.out.println("O terceiro número é o menor: " + n3);
		}else {
			System.out.println("Entrada de dados com números iguais.");
		}
	}
}

/* 8. Faça um programa que pergunte o preço de três produtos e informe qual produto você
* deve comprar, sabendo que a decisão é sempre pelo mais barato.
*/

import java.util.Scanner;

public class Exercicio8 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
	
		System.out.println("Insira o preço de 3 produtos: ");
		double preco1 = input.nextDouble();
		double preco2 = input.nextDouble();
		double preco3 = input.nextDouble();
		
		if (preco1 < preco2 && preco1 < preco3) {
			System.out.println("Compre o primeiro produto, pois ele é mais barato.");
		} else if (preco2 < preco1 && preco2 < preco3) {
			System.out.println("O segundo produto é mais barato.");
		} else if (preco3 < preco1 && preco3 < preco2) {
			System.out.println("O terceiro produto é mais barato.");
		}else {
			System.out.println("Entrada de dados com números iguais.");	
		
		}
	}
}

/* 9. Faça um Programa que leia três números e mostre-os em ordem decrescente.
*/

import java.util.Scanner;

public class Exercicio9 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Insira 3 número inteiros:");
		int n1 = input.nextInt();
		int n2 = input.nextInt();
		int n3 = input.nextInt();
		
		//usando if:
		
		if (n1 <= n2 && n1 <= n3 && n2 <= n3) { // 1, 2, 3
			System.out.println(n1 + "," + n2 +  "," + n3);
			
		}else if (n1 <= n2 && n1 <= n3 && n3 <= n2) { // 1,3,2
			System.out.println(n1 + "," + n3 +  "," + n2);
			
		}else if (n3 <= n2 && n1 <= n2 && n3 <= n1) { // 2, 3 1
			System.out.println(n3 + "," + n1 +  "," + n2);
			
		}else if (n2 <= n1 && n2 <= n3 && n1 <= n3) { // 2,1,3
			System.out.println(n2 + "," + n3 +  "," + n1);
			
		}else if (n3 <= n1 && n2 <= n1 && n3 <= n2) { // 3,2,1
			System.out.println(n3 + "," + n2 +  "," + n1);

		}else if (n3 <= n1 && n2 <= n1 && n2 <= n3) { // 3,1,2
				System.out.println(n2 + "," + n3 +  "," + n1);
		}
	}
}

/* 10. Faça um Programa que pergunte em que turno você estuda. Peça para digitar M- Matutino ou V- Vespertino ou N- Noturno. Imprima a mensagem "Bom Dia!", "Boa
* Tarde!" ou "Boa Noite!" ou "Valor Inválido!", conforme o caso.
*/

import java.util.Scanner;

public class Exercicio10 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe o turno que você estuda. \n M - Matutino \n V - Vespertino \n N- Noturno");
		String turno = input.nextLine();
		
		switch(turno) {
		case "M", "m": System.out.println("Bom dia!");{
		}break;
		case "V", "v": System.out.println("Boa tarde!");{
		}break;
		case "N", "n": System.out.println("Boa noite!");{
		}break;
		default: System.out.println("Dado inválido."); 
		
		}
	}
}

/* 11. As Organizações Tabajara resolveram dar um aumento de salário aos seus
* colaboradores e lhe contaram para desenvolver o programa que calculará os reajustes.
* Faça um programa que recebe o salário de um colaborador e o reajuste segundo o
* seguinte critério, baseado no salário atual: o salários até R$ 280,00 (incluindo):
* aumento de 20% o salários entre R$ 280,00 e R$ 700,00 : aumento de 15% o salários
* entre R$ 700,00 e R$ 1500,00 : aumento de 10% o salários de R$ 1500,00 em diante
* : aumento de 5% Após o aumento ser realizado, informe na tela:
* - o salário antes do reajuste;
* - o percentual de aumento aplicado;
* - o valor do aumento;
* - o novo salário, após o aumento.
*/

import java.util.Scanner;

public class Exercicio11 {
	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
	
		System.out.println(" Informe o salário base: ");
		double salario = input.nextDouble();
		
		if (salario <= 280) {
			double salarioR = (salario*0.2)+salario;
			System.out.println(" Seu salário base é de R$" + salario +"\n Com a porcentagem de aumento de 20%, o novo salário será de: R$" + salarioR);
		}else if (salario >= 280 && salario <= 700) {
			double salarioR = (salario*0.15)+salario;
			System.out.println(" Seu salário base é de R$" + salario + "\n Com a porcentagem de aumento de 20%, o novo salário será de: R$" + salarioR);
		}else if (salario >= 700 && salario <= 1500) {
			double salarioR = (salario*0.1)+salario;
			System.out.println(" Seu salário base é de R$" + salario + "\n Com a porcentagem de aumento de 20%, o novo salário será de: R$" + salarioR);
		}else {
			double salarioR = (salario*0.05)+salario;
			System.out.println(" Seu salário base é de R$" + salario + "\n Com a porcentagem de aumento de 20%, o novo salário será de: R$" + salarioR);
	
		}
	}
}

/* 12. Faça um programa para o cálculo de uma folha de pagamento, sabendo que os
* descontos são do Imposto de Renda, que depende do salário bruto (conforme tabela
* abaixo) e 3% para o Sindicato e que o FGTS corresponde a 11% do Salário Bruto, mas
* não é descontado (é a empresa que deposita). O Salário Líquido corresponde ao
* Salário Bruto menos os descontos. O programa deverá pedir ao usuário o valor da sua
* hora e a quantidade de horas trabalhadas no mês.
* - o desconto do IR: o Salário Bruto até 900 (inclusive) == isento;
* - Salário Bruto até 1500 (inclusive) - desconto de 5%
* - Salário Bruto até 2500 (inclusive) - desconto de 10%
* - Salário Bruto acima de 2500 - desconto de 20%
* Imprima na tela as informações, dispostas conforme o exemplo abaixo. No exemplo
* o valor da hora é 5 e a quantidade de hora é 220.
* Salário Bruto: (5 * 220) : R$ 1100,00
* (-) IR (5%) : R$ 55,00
* (-) INSS ( 10%) : R$ 110,00 FGTS (11%) : R$ 121,00
* Total de descontos : R$ 165,00
* Salário Liquido : R$ 935,00
*/

import java.util.Scanner;

public class Exercicio12 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe quantas horas você trabalha por mês: ");
		int horasTrabalhadas = input.nextInt();
		System.out.println("Informe quanto você ganha por hora: ");
		double salarioHoras = input.nextDouble();
		
		double salarioBruto = salarioHoras*horasTrabalhadas;
		double sindicato = salarioBruto*0.03;
		double inss = salarioBruto*0.1;
		double fgts = salarioBruto*0.11;
		
		double percentualIR = 0;
		
		if (salarioBruto <= 1500) {
			percentualIR = 0.05;
		}else if (salarioBruto <= 2500){
			percentualIR = 0.1;
		}else if (salarioBruto > 2500){
			percentualIR = 0.2;
		}else {
			percentualIR = 0;
		}
		
		double ir = salarioBruto*percentualIR;
		double totalDescontos = sindicato + inss + ir;
		double salarioLiquido = salarioBruto - totalDescontos;
		
		System.out.println("Salário Bruto: "+ "(" + salarioHoras + " * " + horasTrabalhadas + ")" + ": R$" + salarioBruto);
		System.out.println("(-) IR (5%) " + ": R$" + ir);
		System.out.println("(-) INSS (10%) " + ": R$" + inss);
		System.out.println("FGTS (11%) " + ": R$" + fgts);
		System.out.println("Sindicato (3%) " + ": R$" + sindicato);
		System.out.println("Total de descontos" + ": R$" + totalDescontos);
		System.out.println("Salário Líquido " + ": R$" + salarioLiquido);

	}
}

/* 13. Faça um Programa que leia um número e exiba o dia correspondente da semana. (1-
* Domingo, 2- Segunda, etc.), se digitar outro valor deve aparecer valor inválido.
*/ 

import java.util.Scanner;

public class Exercicio13 {

	public static void main(String[] args) {
		Scanner scan = new Scanner (System.in);
		
		System.out.println("Entre com um dia da semana (1-7): ");
		int diaSemana = scan.nextInt();
		
		switch (diaSemana) {
		case 1: System.out.println("Domingo"); break;
		case 2: System.out.println("Segunda"); break;
		case 3: System.out.println("Terça"); break;
		case 4: System.out.println("Quarta"); break;
		case 5: System.out.println("Quionta"); break;
		case 6: System.out.println("Sexta"); break;
		case 7: System.out.println("Sábado"); break;
		default: System.out.println("Não é um dia da semana.");		
			
		}
	}
}

/*14. Faça um programa que lê as duas notas parciais obtidas por um aluno numa disciplina
* ao longo de um semestre, e calcule a sua média. A atribuição de conceitos obedece à
* tabela abaixo: o Média de Aproveitamento Conceito o Entre 9.0 e 10.0 A o Entre 7.5 e
* 9.0 B o Entre 6.0 e 7.5 C o Entre 4.0 e 6.0 D o Entre 4.0 e zero E O algoritmo deve
* mostrar na tela as notas, a média, o conceito correspondente e a mensagem
*“APROVADO” se o conceito for A, B ou C ou “REPROVADO” se o conceito for D ou E.
*/

import java.util.Scanner;

public class Exercicio14 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe a primeira nota: ");
		double n1 = input.nextInt();
		System.out.println("Informe a segunda nota: ");
		double n2 = input.nextDouble();
		
		double media = (n1+n2)/2;
		String conceito = null;
		
		if (n1 >= 9 && n1 == 10 && n2 >=9 && n2 == 10) {
			conceito = "A";
		}else if (n1 >= 7.5 && n1 == 9 && n2 >= 7.5 && n2 == 9) {
			conceito = "B";
		}else if (n1 >= 6 && n1 == 7.5 && n2 >= 6 && n2 == 7.5) {
			conceito = "c";
		}else if (n1 >= 4 && n1 == 6 && n2 >= 4 && n2 == 6) {
				conceito = "D";
		}else if (n1 >= 0 && n1 == 4 && n2 >= 0 && n2 == 4) {
					conceito = "E";
		}
		
		System.out.println("Média = " + media);
		
		switch (conceito){
		case "A", "B" , "C": System.out.println("APROVADO");{	
		}break;
		case "D" , "E": System.out.println("REPROVADO");{
		}break;
			
			
		}
	}
}

/* 15. Faça um Programa que peça os 3 lados de um triângulo. O programa deverá
* informar se os valores podem ser um triângulo. Indique, caso os lados formem um
* triângulo, se o mesmo é: equilátero, isósceles ou escaleno.
* - Dicas: Três lados formam um triângulo quando a soma de quaisquer dois lados
* for maior que o terceiro;
* - Triângulo Equilátero: três lados iguais;
* - Triângulo Isósceles: quaisquer dois lados iguais;
*- Triângulo Escaleno: três lados diferentes;
*/

import java.util.Scanner;

public class Exercicio15 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe a medida de cada lado do trinângulo: ");
		double n1 = input.nextDouble();
		double n2 = input.nextDouble();
		double n3 = input.nextDouble();
		
		//todos os lados iguais
		if ( n1 == n2 && n1 == n3 && n2 == n3) {
			System.out.println("É um triangulo Equilátero");
		//pelo menos dois lados iguais 
		}else if (n1 == n2 || n1 == n3 || n2 == n3) {
				System.out.println("É um triangulo Isósceles.");
		//todos os lados diferentes
		}else {
			System.out.println("É um um triangulo Escaleno");
			
		}
	}
}

/* 16. Faça um programa que calcule as raízes de uma equação do segundo grau, na forma
* ax2 + bx + c. O programa deverá pedir os valores de a, b e c e fazer as consistências,
* informando ao usuário nas seguintes situações:
* a. Se o usuário informar o valor de A igual a zero, a equação não é do segundo
* grau e o programa não deve fazer pedir os demais valores, sendo encerrado;
* b. Se o delta calculado for negativo, a equação não possui raízes reais. Informe ao
* usuário e encerre o programa;
* c. Se o delta calculado for igual a zero a equação possui apenas uma raiz real;
* informe-a ao usuário;
* d. Se o delta for positivo, a equação possui duas raízes reais; informe-as ao
* usuário;
*/

import java.util.Scanner;
import java.lang.Math;

public class Exercicio16 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		//ax2+bx+c
		//x = (-b +- raiz de delta)/2*a
		//delta = (b)^2 - 4.a.c
		
		System.out.println("Informe os valores de a, b e c: ");
		int a = input.nextInt();
		int b = input.nextInt();
		int c = input.nextInt();
	
		double valorDelta = Math.pow(b, 2) - 4*a*c;
		double valorXSoma= -b + Math.sqrt(valorDelta)/2*a;
		double valorXSubtrai= -b - Math.sqrt(valorDelta)/2*a;
		
		if (a == 0) {
			System.exit(0);
		}else if (valorDelta < 0) {
			System.out.println("O valor de delta não pode ser negativo.");
			System.exit(0);
		}else if (valorDelta == 0) {
			System.out.println("Delta é igual a zero. A equação possui somente uma raíz real: " + valorXSoma + valorXSubtrai );
		}else {
			System.out.println("As possíveis raízes reais são: " + valorXSoma +" e "+ valorXSubtrai);
		
		}
	}
}

/* 17. Faça um Programa que peça um número correspondente a um determinado ano e em
* seguida informe se este ano é ou não bissexto.
*/

import java.util.Scanner;

public class Exercicio17 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		//condição: o ano deve ser bissexto.
		 //condição de ano bissexto: 366 dias.
		  //anos bissextos são divisíveis por 4, 100 e 400 com resto = 0.
		
		System.out.println("Informe um ano para descobrir se é bissexto: ");
		int ano = input.nextInt();
		
		if (ano % 4 == 0 && ano % 100 != 0 || ano % 400 == 0) {
			System.out.println("O ano é bissexto.");
		}else {
			System.out.println("Não é ano bissexto.");
			
		}
	}
}

/* 18. Faça um Programa que peça uma data no formato dd/mm/aaaa e determine se a
* mesma é uma data válida.
*/

import java.util.Scanner;

public class Exercicio18 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe uma data no formato dd/mm/aaaa");
		int dia=input.nextInt();
		int mes=input.nextInt();
		String ano=input.next();
	
		if (dia <=1 && dia >=31) {
			System.out.println("O dia não pode ser menor que 1 ou maior que 31.");
		}else if (mes <=1 && mes >=12 ) {
			System.out.println("O mês informado deve estar entre 1 e 12.");
		}else {
			System.out.println("A data é: " + dia + "/" + mes + "/" + ano);

		}
	}
}

/* 19. Faça um Programa que leia um número inteiro menor que 1000 e imprima a quantidade
* de centenas, dezenas e unidades do mesmo. o Observando os termos no plural a
* colocação do "e", da vírgula entre outros. Exemplo: o 326 = 3 centenas, 2 dezenas e 6
* unidades o 12 = 1 dezena e 2 unidades. Testar com: 326, 300, 100, 320, 310,305, 301,
* 101, 311, 111, 25, 20, 10, 21, 11, 1, 7 e 16.
*/

import java.util.Scanner;

public class Exercicio19 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe um número inteiro menor que 1000: ");
		int n = input.nextInt();
		
		double centenas = n/100;
		double dezenas = n/10;
		
		if ( n < 0 && n > 1000 ) {
			System.out.println("Número inválido.");
		}else if (n % 100 == 0) {
			System.out.println("O número tem " + Math.round(centenas) + " centenas.");
		}else if (n % 10 == 0) {
			System.out.println("O número tem " + Math.round(dezenas) + " dezenas.");
			
		}
	}
}

/* 20. Faça um Programa para leitura de três notas parciais de um aluno. O programa deve
* calcular a média alcançada por aluno e presentar: . A mensagem "Aprovado", se a
* média for maior ou igual a 7, com a respectiva média alcançada; a. A mensagem
* "Reprovado", se a média for menor do que 7, com a respectiva média alcançada; b. A
* mensagem "Aprovado com Distinção", se a média for igual a 10.
*/

import java.util.Scanner;

public class Exercicio20 {

	public static void main(String[] args) {
		
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe a 1º nota (entre 0 e 10), a 2º nota (entre 0 e 10) e a 3º (entre 0 e 10) : ");
		double n1 = input.nextDouble();
		double n2 = input.nextDouble();
		double n3 = input.nextDouble();
		
		double media = (n1+n2+3)/3;
				
		if (media == 10) {
			System.out.println("Aprovado com distinção.");
		}else if (media >= 7 ) {
			System.out.println("Aprovado.");
		}else {
			System.out.println("Reprovado.");
			
		}
	}
}

/* 21. Faça um Programa para um caixa eletrônico. O programa deverá perguntar ao usuário
* a valor do saque e depois informar quantas notas de cada valor serão fornecidas. As
* notas disponíveis serão as de 1, 5, 10, 50 e 100 reais. O valor mínimo é de 10 reais e
* o máximo de 600 reais. O programa não deve se preocupar com a quantidade de notas
* existentes na máquina.
* Exemplo 1: Para sacar a quantia de 256 reais, o programa fornece duas notas de 100,
* uma nota de 50, uma nota de 5 e uma nota de 1;
* Exemplo 2: Para sacar a quantia de 399 reais, o programa fornece três notas de 100, uma
* nota de 50, quatro notas de 10, uma nota de 5 e quatro notas de 1.
*/

import java.util.Scanner;

public class Exercicio21 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Bem vindo ao caixa 24H! \n Informe o valor do saque:");
		int valorSaque = input.nextInt();
		
		
		int notas100 = (valorSaque-(valorSaque%100))/100;
		int notas50 = (valorSaque%100)/50;
		int notas10 = (valorSaque%50)/10;
		
		if (valorSaque <= 9 || valorSaque >= 599){
			do {
			
			System.out.println("Valor inválido! Gentileza informe o valor: ");
			valorSaque = input.nextInt();
				
			}while(valorSaque %5 != 0);
				
			}else if (notas100 == 1) {
				System.out.println(notas100 + " notas de R$100");
			}else if (notas100 > 1) {
				System.out.println(notas100 + " notas de R$100");
			}else if (notas50 == 1) {
				System.out.println(notas50 + " notas de R$50");
			}else if (notas50 > 1) {
				System.out.println(notas50 + " notas de R$50");
			}else if (notas10 == 1) {
				System.out.println(notas10 + " notas de R$10");
			}else {
				System.out.println(notas10 + " notas de R$10");
				
			}
		}
	}

/* 22. Faça um Programa que peça um número inteiro e determine se ele é par ou ímpar.
* Dica: utilize o operador módulo (resto da divisão).
*/

import java.util.Scanner;

public class Exercicio22 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Digite o número: ");
		int numero = input.nextInt();
		
		if (numero % 2 !=0) {
			System.out.println("O número é ímpar.");
		}else {
			System.out.println("O número é par.");
			
		}
	}
}

/* 23. Faça um Programa que peça um número e informe se o número é inteiro ou decimal.
* Dica: utilize uma função de arredondamento.
*/

import java.util.Scanner;

public class Exercicio23 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe um número (inteiro ou decimal):");
		double numero = input.nextDouble();
		
		if (numero == Math.round(numero)) {
			System.out.println("É um número inteiro.");
			
		}else {
			System.out.println("É um número decimal.");
			
		}
	}
}

/* 24. Faça um Programa que leia 2 números e em seguida pergunte ao usuário qual
* operação ele deseja realizar. O resultado da operação deve ser acompanhado de uma
* frase que diga se o número é: par ou ímpar;
* a. positivo ou negativo;
* b. inteiro ou decimal.
*/

import java.util.Scanner;

public class Exercicio24 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe dois números: ");
		double n1 = input.nextDouble();
		double n2 = input.nextDouble();
		
		if (n1 % 2 == 0){
			System.out.println("O primerio número é par.");
		}else if (n2 % 2 == 0) {
			System.out.println("O segundo número é par.");
		}else {
			System.out.println("O primeiro e o segundo número são ímpares.");
		}
		
		if (n1 > 0 || n2 > 0) {
			System.out.println("Os números são positivos.");
		}else if(n1 < 0) {
			System.out.println(" O primeiro número é negativo.");
		}else if(n2< 0) {
			System.out.println("O segundo número é negativo.");
		}
		
		if (n1 == Math.round(n1)) {
			System.out.println("O primeiro número é inteiro.");
			
		}else {
			System.out.println("O primeiro número é decimal.");
		}
		
		if (n2 == Math.round(n2)) {
			System.out.println("O segundo número é inteiro.");
				
		}else {
			System.out.println("O segundo número é decimal.");
			
		}
			
		System.out.println("Selecione uma operação: \n(+) SOMAR \n(-)SUBTRAIR");
		String operacao = input.next();
		
		switch (operacao){
		case "SOMAR", "somar" , "+": {
			double soma = n1+n2;
			System.out.println("A soma é: " + soma);
		}break;
		case "SUBTRAIR", "subtrair", "-":{
			double subtracao = n1-n2;
			System.out.println("A subtração é: " + subtracao);
		}break;
		default: {
			System.out.println("Valor inválido.");
			
			}
		}
	}
}

/* 25. Faça um programa que faça 5 perguntas para uma pessoa sobre um crime. As
* perguntas são:
* - “Telefonou para a vítima?" a.
* - "Esteve no local do crime?" b.
* - "Mora perto da vítima?" c.
* - "Devia para a vítima?" d.
* - "Já trabalhou com a vítima?"
* O programa deve no final emitir uma classificação sobre a participação da pessoa no
* crime. Se a pessoa responder positivamente a 2 questões ela deve ser classificada
* como "Suspeita", entre 3 e 4 como "Cúmplice" e 5 como "Assassino". Caso contrário,
* ele será classificado como "Inocente".
*/

import java.util.Scanner;

public class Exercicio25 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		int perfil  = 0;
		
		System.out.println("Responda as perguntas abaixo: \nTelefonou para a vítima?");
		String p1 = input.nextLine();
		
		System.out.println("Esteve no local do crime?");
		String p2 = input.nextLine();
		
		System.out.println("Mora peroto da vítima?");
		String p3 = input.nextLine();
		
		System.out.println("Devia para a vítima?");
		String p4 = input.nextLine();

		System.out.println("Já trabalhou para a vítima?");
		String p5 = input.nextLine();
		
		if (p1.equals("S")){
			perfil ++;
		}if (p2.equals("S")) {
			perfil ++;
		}if(p3.equals("S")){
			perfil++;
		}if (p4.equals("S")){
			perfil++;
		} if (p5.equals("S")) {
			perfil++;
		}

		if (perfil == 2) {
			System.out.println("Você é SUSPEITO.");
		}if (perfil == 3 || perfil == 4) {
			System.out.println("Você é CÚMPLICE.");
		}if (perfil == 5) {
			System.out.println("Você é o ASSASSINO.");
		}if (perfil == 0 ) {
			System.out.println("Você é INOCENTE.");
			
		}
	}
}

/* 26. Um posto está vendendo combustíveis com a seguinte tabela de descontos: .
* - Álcool:
* a. até 20 litros, desconto de 3% por litro                                                                                        
* b. acima de 20 litros, desconto de 5% por litro
* - Gasolina:                                                                                                                                                                               
* c. até 20 litros, desconto de 4% por litro                                                                                        
* d. acima de 20 litros, desconto de 6% por litro.
* - Escreva um algoritmo que leia o número de litros vendidos, o tipo de
* combustível (codificado da seguinte forma: A-álcool, G-gasolina), calcule e imprima o
* valor a ser pago pelo cliente sabendo-se que o preço do litro da gasolina é R$ 2,50 o 
* preço do litro do álcool é R$ 1,90.
*/   
    import java.util.Scanner;
    
    public class Exercicio26 {
    
    	public static void main(String[] args) {
    		Scanner input = new Scanner(System.in);
    		
    		double alcool = 1.90, gasolina = 2.50;
    		
    		System.out.println("Esolha o tipo de combustível: \nA- Álcool \nG- Gasolina");
    		String combustivel = input.nextLine();
    		
    		if(combustivel.equalsIgnoreCase("A")) {
    			System.out.println("Informe quantos litros de álcool deseja comprar:");
    			alcool = input.nextDouble();
    			if (alcool <= 20) {
    				alcool = alcool-(alcool*0.3);
    				System.out.println(alcool);
    			}if (alcool > 20) {
    				alcool = alcool-(alcool*0.5);
    				System.out.println(alcool);
    			}
    		
    		if (combustivel.equalsIgnoreCase("G")){
    			System.out.println("Informe quantos litros de gasolina deseja comprar:");
    			gasolina = input.nextDouble();
    			if (gasolina <=20) {
    				gasolina = gasolina-(gasolina*0.4);
    				System.out.println(gasolina);
    			}if (alcool > 20) {
    				alcool = gasolina-(gasolina*0.6);
    				System.out.println(gasolina);
    				
    				}
    			}
    		}
    	}
    } 
  
/*27. Uma fruteira está vendendo frutas com a seguinte tabela de preços: 
*                 ATÉ 5KG          ACIMA DE 5KG
* MORANGO       R$2,50 POR KG     R$2,20 POR KG
* MAÇÃ          R$1,80 POR KG     R$1,50 POR KG
* Se o cliente comprar mais de 8 Kg em frutas ou o valor total da compra ultrapassar R$
* 25,00, receberá ainda um desconto de 10% sobre este total. Escreva um algoritmo
* para ler a quantidade (em Kg) de morangos e a quantidade (em Kg) de maças
* adquiridas e escreva o valor a ser pago pelo cliente.
*/

import java.util.Scanner;

public class Exercicio27 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
		System.out.println("Informe quantas KG de Maças quer levar: ");
		int maca = input.nextInt();
		System.out.println("Informe quantos KG de Morangos quer levar: ");
		int morango = input.nextInt();
		
		double precoKgMorango = 0;
		if(maca <=5 ) {
			precoKgMorango = 2.5;
		}else {
			precoKgMorango = 2.2;
		}
		
		double precoKgMaca = 0;
		if (maca <=5) {
			precoKgMaca = 1.8;
		}else {
			precoKgMaca = 1.5;
		}
		
		double precoTotalMorango = morango*precoKgMorango;
		double precoTotalMaca = maca*precoKgMaca;
		double precoParcial = precoKgMorango + precoKgMaca;
		double precoTotal = precoParcial;
		
		if(morango + maca > 8 || precoParcial > 25) {
			precoTotal = precoParcial - (precoParcial*0.1);
		}
		
		System.out.println("Preço Total: " + precoTotal);
	}
}

/* 28. O Hipermercado Tabajara está com uma promoção de carnes que é imperdível.
* Confira:
*                 ATÉ 5KG         ACIMA DE 5KG    
* FILÉ DUPLO      R$4,90 POR KG   R$5,80 POR KG
* ALCATRA         R$5,90 POR KG   R$6,80 POR KG
* PICNHA          R$6,90 POR KG   R$7,90 POR KG
* Para atender a todos os clientes, cada cliente poderá levar apenas um dos tipos de carne
* da promoção, porém não há limites para a quantidade de carne por cliente. Se compra for
* feita no cartão Tabajara o cliente receberá ainda um desconto de 5% sobre o total a
* compra. Escreva um programa que peça o tipo e a quantidade de carne comprada pelo
* usuário e gere um cupom fiscal, contendo as informações da compra: tipo e quantidade
* de carne, preço total, tipo de pagamento, valor do desconto e valor a pagar.
*/

import java.util.Scanner;

public class Exercicio28 {

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		
			System.out.println("Entre com o tipo da carne: ");
			System.out.println("1 - Filé Dplo ");
			System.out.println("2 - Alcatra ");
			System.out.println("3 - Picanha ");
			int tipo = input.nextInt();
			
			System.out.println("Entre com a quantidade (kg): ");
			double qtd = input.nextDouble();
			
			double precoKg = 0;
			
			if(tipo == 1) {	
				System.out.println(qtd + "kg - Filé Duplo");
				if (qtd < 5) {
					precoKg = 4.9;
				}else {
					precoKg = 5.8;
				}
			}
				
			if(tipo == 2) {
				System.out.println(qtd + "kg - Alcatra");
				if (qtd < 5) {
					precoKg = 5.9;
				}else {
					precoKg = 6.8;
				}
			}
			
			if(tipo == 3) {
				System.out.println(qtd + "kg - Picanha");
				if (qtd < 5) {
					precoKg = 6.9;
				}else {
					precoKg = 7.8;
				}
			}
			
		double total = qtd * precoKg;
		System.out.println(qtd + " + " + precoKg + " = " + total);
		
		System.out.println("Compra no cartão? Digite 1 para sim. ");
		int cartao = input.nextInt();
		
		if (cartao == 1) {
			double desconto = total*0.05;
			System.out.println("Desconto de: " + desconto);
			System.out.println("Valor a pagar: " + (total - desconto));
		}else {
			System.out.println("Valor a pagar: " + total);

		}
	}		
}
```