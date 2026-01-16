package grudve;

import java.util.Random;
public class SneskoMain {
	public static void main(String[] args) {
		Snesko s1 =  new Snesko("Belic",2.5,15.0);
		Snesko s2 = new Snesko("Delic",3.0,12.0);
		
		Random random =  new Random();
		
		
		Snesko naPotezu = s1;
		Snesko meta = s2;
		System.out.println("SNESKO 1 " +naPotezu.getNaziv()+" SA SNAGOM "+naPotezu.snaga+" I "+naPotezu.getBrZivota()+" HP-a");
		System.out.println("SNESKO 2 " +meta.getNaziv()+" SA SNAGOM "+	meta.snaga+" I "+meta.getBrZivota()+" HP-a");
		while(naPotezu.siZiv()) {
			double verovatnocaPogotka = random.nextDouble();
			System.out.println(naPotezu.getNaziv()+" gadja "+meta.getNaziv());
			if(verovatnocaPogotka >= 0.5) {
				if(verovatnocaPogotka >= 0.75) {
					naPotezu.gadjajCrit(meta);
					System.out.println("pogodak i gadja opet CRITICAL");
					System.out.println(meta.getNaziv()+" ima jos "+meta.getBrZivota()+" hp-a");
				}
				naPotezu.gadjaj(meta);
				System.out.println("pogodak");
				System.out.println(meta.getNaziv()+" ima jos "+meta.getBrZivota()+" hp-a");
			} else System.out.println("za malo");
			
		Snesko tmp = naPotezu;
		naPotezu = meta;
		meta = tmp;
		}
		if(meta.siZiv() == true) {
			System.out.println("Pobednik je "+naPotezu.getNaziv()+" CESTITAMO");
		}
	}
}