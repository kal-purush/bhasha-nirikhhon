import java.util.Random;
import java.util.Scanner;

public class Hyaku2{
	public static void main(String[] args){
		String[][] word = {
			{"白露に風の吹きしく秋の野は", "つらぬきとめぬ玉ぞ散りける"},
			{"忘らるる身をば思はずちかひてし", "人のいのちの惜しくもあるかな"},
			{"浅茅生の小野の篠原しのぶれど", "あまりてなどか人の恋しき"},
			{"しのぶれど色にいでにけりわが恋は", "物や思ふと人のとふまで"},
			{"恋すてふわが名はまだき立ちにけり", "人しれずこそ思ひそめしか"},
			{"ちぎりきなかたみに袖をしぼりつつ", "末の松山波こさじとは"},
			{"あひみてののちの心にくらぶれば", "昔は物を思はざりけり"},
			{"あふことのたえてしなくはなかなかに", "人をも身をも恨みざらまし"},
			{"あはれともいふべき人は思ほえで", "身のいたづらになりぬべきかな"},
			{"由良のとをわたる舟人かぢを絶え", "ゆくへも知らぬこひの道かな"},
			{"八重むぐらしげれる宿のさびしきに", "人こそ見えぬ秋は来にけり"},
			{"風をいたみ岩うつ波のおのれのみ", "くだけて物を思ふころかな"},
			{"みかきもり衛士のたく火の夜は燃え", "昼は消えつつ物をこそ思へ"},
			{"君がため惜しからざりし命さへ", "長くもがなと思ひけるかな"},
		};

		int[] array = new int[word.length];
		for (int i = 0; i < array.length; i++) {
			array[i] = i;
		}
		int[] check = new int[word.length];
		for (int i = 0; i < check.length; i++) {
			check[i] = i;
		}

		Scanner stdIn = new Scanner(System.in);

		while(true){
			shuffle(array, array.length);
			shuffle(check, check.length);
			for(int i = 0; i < array.length; i++){
				for (int j = 0; j < check.length; j++) {
					System.out.println(j+word[check[j]][1]);
				}
				System.out.println(word[array[i]][0]);
				int input = stdIn.nextInt();
				if (array[i] == check[input]) {
					System.out.println("正解");
				}else{
					System.out.println("不正解");
				}
				System.out.println(word[array[i]][0]+" "+word[array[i]][1]+"\n");
			}
			System.out.println("LOOP");
		}
	}

	public static void shuffle(int[] array, int times){
		Random rand = new Random();
		for (int i = 0; i < times; i++) {
			int j = rand.nextInt(array.length);
            int k = j;
            while(k == j){
                j = rand.nextInt(array.length);
            }
            int buffer = array[j];
            array[j] = array[k];
            array[k] = buffer;
		}
	}
}