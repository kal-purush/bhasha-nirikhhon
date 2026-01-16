using UnityEngine;
using System.Collections;

public class Item {

	public enum Type {
		knife,
		bag,
		chocolate,
	};

	private static string SPRITES_FOLDER = "Sprites/";

	public static Sprite knifeSprite =  null;
	public static Sprite bagSprite =  null;
	public static Sprite chocolateSprite =  null;

	private static int knifeValue 		= 5;
	private static int bagValue 		= 2000;
	private static int chocolateValue 	= 10;

	private static int knifeHp 		= 10;
	private static int bagHp 		= 10;
	private static int chocolateHp 	= 1;


	public static int getValue(Type type){
		switch(type){
		case Type.knife: return knifeValue;
		case Type.bag: return bagValue;
		case Type.chocolate: return chocolateValue;

		default: return 0;
		}
	}

	public static int getHp(Type type){
		switch(type){
		case Type.knife: return knifeHp;
		case Type.bag: return bagHp;
		case Type.chocolate: return chocolateHp;

		default: return 0;
		}
	}

	public static Sprite getSprite(Type type){
		switch(type){
		case Type.bag: return getSprite ("bag");
		case Type.chocolate: return getSprite ("chocolate");
		case Type.knife:
		default: return getSprite("knife");
		}
	}

	private static Sprite getSprite(string spriteName){
		return Resources.Load (SPRITES_FOLDER + spriteName, typeof(Sprite)) as Sprite;
	}

}