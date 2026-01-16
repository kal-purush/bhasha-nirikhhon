package battle;
import character.Enemy;
import character.Player;

import java.util.Scanner;


public class PrintBattle {


	public static void runBattle(Player player,Enemy enemy){

		while(!judgeParam.zeroHp(player)){

			Scanner scan = new Scanner(System.in);

			System.out.println(player.getName() + "のHP：" + player.getHp());

			if( player.getSpeed() >= enemy.getSpeed()){
					oneTurn(enemy,player);
				if(!judgeParam.zeroHp(enemy)){
					oneTurn(player,enemy);
				}else{
					System.out.println("たおした");
					return;
				}
			} else
				{
					oneTurn(player,enemy);
					if(!judgeParam.zeroHp(player)){
						oneTurn(enemy,player);
					}else{
						System.out.println(player.getName() + "はたおれた");
						return;
					}
			}
			System.out.println(player.getName() + "のHP：" + player.getHp());



			System.out.println("続けるには１を，終わるには１以外の任意の整数を入力してください。");
			int temp = scan.nextInt();
			System.out.println(temp);

			if(temp == 1){
				//NR
			}else{
				return;
			}



		}


		
	}

	public static void oneTurn(Player attacked,Enemy attacking){
		System.out.println(attacking.getName() + "の攻撃！");
		CalcDamage.attackCalc(attacked,attacking);
		System.out.println(attacked.getName() + "は" + CalcDamage.damage + "のダメージを受けた");
	}

	public static void oneTurn(Enemy attacked,Player attacking){
		System.out.println(attacking.getName() + "の攻撃！");
		CalcDamage.attackCalc(attacked,attacking);
		System.out.println(attacked.getName() + "は" + CalcDamage.damage + "のダメージを受けた");
	}


}