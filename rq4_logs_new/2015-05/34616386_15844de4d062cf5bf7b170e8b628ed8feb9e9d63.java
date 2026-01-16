public class tebakgambar{
	public static void main (String[] args) {
		String []label={"gambar","angka"};
		int []score={0,0};
		//lakukan 12x lempar koin
		System.out.println("Ke\t| Hasil Lemparan");
		for (int i=0; i<12; i++){
			int r= (int)(Math.random()+0.5);
				System.out.println((i+1)+"\t|\t" + label[r]);
				score[r]++;
		}
		System.out.println("==================================");
		// hitunng score 0 - gambar score 1 angkat
		//
		for(int i=0; i<label.length; i++){
			System.out.println("Score" + label[i]+"="+score[i]);
			}
			if (score[]);
				{
			}
			
	}
}