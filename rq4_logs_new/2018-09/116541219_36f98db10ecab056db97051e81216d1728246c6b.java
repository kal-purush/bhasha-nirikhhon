package examples.Lotto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LottoMachineImpl implements LottoMachine {
	private List<LottoBall> balls;

	@Override
	public void put(List<LottoBall> balls) {
		this.balls = balls;
	}

	@Override
	public void mix() {
		Collections.shuffle(balls);
		
	}

	@Override
	public List<LottoBall> getBalls() {
		List<LottoBall> result = new ArrayList<LottoBall>();
		for(int i=0; i < 6 ; i++) {
			result.add(balls.get(i));
		}
		return result;
	}
	
}