package br.com.dio.calculadora;

import java.util.Scanner;

public class Calculadora {

	public static void main(String[] args) {
		
		Scanner scan = new Scanner (System.in);
		
		int a, b;
		
		System.out.println("Digite o primeiro valor: ");
		a = scan.nextInt();
		System.out.println("Digite o segundo valor: ");
		b = scan.nextInt();
		
		int soma = soma(a,b);
		int subtracao = subtracao(a,b);
		int multiplicao = multiplicao(a,b);
		float divisao = divisao(a,b);
		
		System.out.println("Seu resultado da soma foi: " + soma);
		System.out.println("Seu resultado da subtracao foi: " + subtracao);
		System.out.println("Seu resultado da multiplicao foi: " + multiplicao);
		System.out.println("Seu resultado da divisao foi: " + divisao);
	}
	
	public static int soma(int a, int b) {
		return a + b;
	}
	
	public static int subtracao(int a, int b) {
		return a - b;
	}
	
	public static int multiplicao(int a, int b) {
		return a * b;
	}
	
	public static float divisao(float a, float b) {
		return a / b;
	}

}