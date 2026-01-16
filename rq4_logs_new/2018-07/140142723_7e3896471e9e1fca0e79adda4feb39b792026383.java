package jp.co.gaiarai.main;

import jp.co.gaiarai.obj.Hero;
import jp.co.gaiarai.obj.Matango;
import jp.co.gaiarai.obj.Sword;

public class Main {

	public static void main(String[] args) {
		//
		Sword s = new Sword();
		s.name = "炎の剣";
		s.damage = 10;
		//勇者よ、この仮想世界に生まれよ！
		Hero h = new Hero();
		//h.name = "ミナト";
		//h.hp = 100;
		h.sword = s;
		//お化けキノコよ、この仮想世界に生まれよ！
		Matango m1 = new Matango();
		m1.hp = 50;
		m1.suffix = 'A';

		Matango m2 = new Matango();
		m2.hp = 48;
		m2.suffix = 'B';
		System.out.println(h.sword.name);
	}
}