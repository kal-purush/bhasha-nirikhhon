
public class Animal {
	
	protected int x;
	protected int y;
	protected int eatMove;
	protected int surviveMove;
	protected String type;
	
	public Animal(String type) {
		int a = (int)(Math.random()*Ecosystem.HEIGHT);
		int b = (int)(Math.random()*Ecosystem.WIDTH);
		while(!check(a,b)) {						//while check a, b is false
			a = (int)(Math.random()*Ecosystem.HEIGHT);
			b = (int)(Math.random()*Ecosystem.WIDTH);
		}
		this.x=a;
		this.y=b;
		if(type.equals("prey")) {
			this.type = type;
		}
		if(type.equals("predator")) {
			this.type = type;
		}
		this.eatMove = 0;
		this.surviveMove = 0;
	}
	
	public Animal(int a, int b, String type) {
		this.x=a - (a%Ecosystem.GWIDTH);
		this.y=b - (b%Ecosystem.GWIDTH);
		if(type.equals("prey")) {
			this.type = type;
		}
		if(type.equals("predator")) {
			this.type = type;
		}
		this.eatMove = 0;
		this.surviveMove = 0;
	}

	public boolean check(int a, int b) {
		
		for(Animal anim: Ecosystem.animals) {
			if(a == anim.x) return false;
			if(b == anim.y) return false;
		}
		return true;
	}
	
	public void move() {
		//within this call breed if eligible
		int a = (int)(Math.random()*4);
		if(a==0) {										//north
			this.x = this.x-Ecosystem.GWIDTH;
			if(!check(this.x, this.y)) this.eatNorth();
		}
		else if(a==1) {									//south
			this.x = this.x+Ecosystem.GWIDTH;
			if(!check(this.x, this.y)) this.eatSouth();
		}
		else if(a==2) {									//east
			this.x = this.y-Ecosystem.GWIDTH;
			if(!check(this.x, this.y)) this.eatEast();
		}
		else {											//west
			this.x = this.y+Ecosystem.GWIDTH;
			if(!check(this.x, this.y)) this.eatWest();
		}
		
		if(type.equals("prey")) {
			if( surviveMove==3) this.breed();
		}
	
		if(type.equals("predator")) {
			if( surviveMove==8) this.breed();
			if( eatMove==5) Ecosystem.animals.remove(Ecosystem.animals.indexOf(this));
		}
		surviveMove++;
		eatMove++;
		/*if(this.x >800) this.x = this.x -800;
		if(this.y >800) this.y = this.y -800;
		if(this.x <0) this.x = this.x + 800;
		if(this.y <0) this.y = this.y +800;
		*/
		
		
	}
	public void breed() {
		this.surviveMove =0;
		int a = (int)(Math.random()*4);
		if(a==0) {										//north
			if(check(this.x-Ecosystem.GWIDTH, this.y)) Ecosystem.animals.add(new Animal(this.x-Ecosystem.GWIDTH, this.y, this.type));
		}
		else if(a==1) {									//south
			if(check(this.x+Ecosystem.GWIDTH, this.y)) Ecosystem.animals.add(new Animal(this.x+Ecosystem.GWIDTH, this.y, this.type));
		}
		else if(a==2) {									//east
			if(check(this.x, this.y+Ecosystem.GWIDTH)) Ecosystem.animals.add(new Animal(this.x, this.y+Ecosystem.GWIDTH, this.type));
		}
		else {											//west
			if(check(this.x, this.y-Ecosystem.GWIDTH)) Ecosystem.animals.add(new Animal(this.x, this.y-Ecosystem.GWIDTH, this.type));
		}		
	}
	
	public void eatNorth() {
		for(int i=0; i<Ecosystem.animals.size(); i++) {
			if(Ecosystem.animals.get(i).x == this.x && Ecosystem.animals.get(i).y == this.y) {
				if(Ecosystem.animals.get(i).type.equals("prey")){
			//		Ecosystem.animals.remove(i);
					this.eatMove =0;
					break;
				}
				else {
					this.x = this.x + Ecosystem.GWIDTH;
					break;
				}
			}	
		}
	}
	public void eatSouth() {
		for(int i=0; i<Ecosystem.animals.size(); i++) {
			if(Ecosystem.animals.get(i).x == this.x && Ecosystem.animals.get(i).y == this.y) {
				if(Ecosystem.animals.get(i).type.equals("prey")){
				//	Ecosystem.animals.remove(i);
					this.eatMove =0;
					break;
				}
				else {
					this.x = this.x - Ecosystem.GWIDTH;
					break;
				}
			}	
		}
	}	
	public void eatEast() {
		for(int i=0; i<Ecosystem.animals.size(); i++) {
			if(Ecosystem.animals.get(i).x == this.x && Ecosystem.animals.get(i).y == this.y) {
				if(Ecosystem.animals.get(i).type.equals("prey")) {
				//	Ecosystem.animals.remove(i);
					this.eatMove =0;
					break;
				}
				else {
					this.y = this.y + Ecosystem.GWIDTH;
					break;
				}
			}	
		}
	}	
	public void eatWest() {
		for(int i=0; i<Ecosystem.animals.size(); i++) {
			if(Ecosystem.animals.get(i).x == this.x && Ecosystem.animals.get(i).y == this.y) {
				if(Ecosystem.animals.get(i).type.equals("prey")) {
				//	Ecosystem.animals.remove(i);
					this.eatMove=0;
					break;
				}
				else {
					this.y = this.y - Ecosystem.GWIDTH;
					break;
				}
			}	
		}
	}
	
}



/*
can only move NSEW
ca only breed NSEW

5 moves without eating predator dies from starvation, predator survives 8 moves then it breeds
prey survives 3 turns it breeds
breeding rules: -if you choose the box and its full it doesnt breed


jfjdk


swing/gridworld

start out with an overabundance of prey
*/