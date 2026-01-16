import java.util.Random;

public class Lottery_539 {
	
	static int rounds = 0 ;//回合數
	static int number[] = new int[5] ;//開獎號碼
	static int chosen[] = new int[5] ;//投注號碼
	
	public static void main(String[] args) {
		
		while(rounds < 1000){//模擬1000次
			
			chosen = pop_number();//電腦選號
			number= pop_number();//開獎
			
			int hit = 0;//對中號碼數
			int award = 0;//中獎金額
			
			for( int x = 0; x < 5; ++x ){
				for( int b = 0 ; b < 5 ; b++ ){
					if(chosen[x]==number[b])hit++;
				}
			}
			switch(hit){
				case 5:
					award = award + 8000000 ;
					break;
				case 4:
					award = award + 20000 ;
					break;
				case 3:
					award = award + 300 ;
					break;
				case 2:
					award = award + 50 ;
					break;
				default:
					break;
			}
			
			System.out.print((rounds+1)+".\t");
			System.out.println(" award="+award);
			rounds++;
			hit=0;
			award=0;
		}
	}
	
	public static int[] pop_number(){//隨機自1~39中選5個不同的號碼並排序
		 int result[] = new int[5] ;
		for ( int i = 0; i < 5; ++i ){
			result[i] = rand();
				for ( int j = 0; j < i; ++j ){
					if(result[i] == result[j]){
						i--;
						break;
					}
				}
		}
		for ( int z = 0; z < 5; ++z ){
				for ( int y = 0; y < 4; ++y ){
					if(result[y] >= result[y+1]){
						int tmp = result[y] ;
						result[y] = result[y+1] ;
						result[y+1] = tmp ;
					}
				}
		}
		return result;
	}
	
	public static int rand(){//自1~39中產出隨機整數
		Random r = new Random();
		int code = r.nextInt(39)+1;
		return code;
	}	
}