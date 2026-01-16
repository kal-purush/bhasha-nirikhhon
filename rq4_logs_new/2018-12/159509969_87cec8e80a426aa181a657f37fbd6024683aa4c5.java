
public class Pair {
	public int h;   // coordinate of height
	public int w;	// coordinate of width 
	Pair(int h, int w){
		this.h=h;
		this.w=w;
	}
	static int dist(Pair a, Pair b) {
		int disth=b.h-a.h;
		int distw=b.w-a.w;
		return disth*disth+distw*distw;
	}
	
	public boolean equals(Object o) {
		Pair p=(Pair) o;
		return p.h==this.h&&p.w==this.w;
	}
	public String toString() {
		return "("+h+" , "+w+ ")";
	}
}