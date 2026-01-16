package examples.templateMethod.Battle;

public abstract class Person {
	
	// 전투 준비
	public void readyToBattle() {
		startBodyTraining();
		prepareWeapon();
		prepareArmor();
		if(isUsingPrepareOther() ) {
			prepareOther();
		}
	}
	
	
	// 상속 받은 클래스에서 수정할 수 없도록 final 키워드를 붙였습니다.
	final void startBodyTraining() {
		System.out.println("체력을 회복합니다.");
	}
	
	// 무기를 손질할 때 사용합니다.
	abstract void prepareWeapon();
	
	
	// 뭔가를 걸칠 때 사용합니다.
	abstract void prepareArmor();
	
	
	// 만약 다른 무언가를 사용하려면 true로 바꿔야 합니다.
	// 이 메소드는 "후킹(hooking)" 용도로 사용합니다.
	boolean isUsingPrepareOther() {
		return false;
	}
	
	// 다른 무언가가 필요하면 사용합니다.
	// isUsingPrepareOther 값에 의해 오버라이드 해서 사용 합니다.
	void prepareOther() {};

}