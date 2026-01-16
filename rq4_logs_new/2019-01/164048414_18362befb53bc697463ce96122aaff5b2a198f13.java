
public class CoolSatelite extends Satelite  implements Cooler {
	Boolean isCooling=false;

	CoolSatelite(){
		this.switchCooler(true);
	}
	CoolSatelite(String name){
		super(name);
		this.switchCooler(true);
	}


	public boolean isCool() {
		// TODO 自動生成されたメソッド・スタブ
		return this.isCooling;
	}

	@Override
	public boolean switchCooler() {
		this.isCooling = !this.isCooling;
		// TODO 自動生成されたメソッド・スタブ
		return this.isCooling;
	}

	@Override
	public boolean switchCooler(Boolean isCooling) {
		this.isCooling = isCooling;
		// TODO 自動生成されたメソッド・スタブ
		return this.isCooling;
	}


}