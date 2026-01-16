package puyo.client3;

import static mockit.Deencapsulation.invoke;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;

import puyo.data.Cluster;
import puyo.data.Puyo;
import puyo.data.PuyoState;

public class PuyoPuyoAIImplTest {
	
	private static Gson gson = new Gson();
	
	@Test
	public void test() {
		PuyoPuyoAIImpl ai = new PuyoPuyoAIImpl();
		
		Puyo[][] arr = gson.fromJson("[[\"GREEN\",\"YELLOW\",\"BLUE\",\"PURPLE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\"],[\"YELLOW\",\"YELLOW\",\"PURPLE\",\"YELLOW\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\"],[\"GREEN\",\"RED\",\"YELLOW\",\"YELLOW\",\"GREEN\",\"PURPLE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\"],[\"GREEN\",\"GREEN\",\"RED\",\"RED\",\"RED\",\"GREEN\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\"],[\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\"],[\"RED\",\"BLUE\",\"YELLOW\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\",\"NONE\"]]", Puyo[][].class);
		Cluster currentPuyo = new Cluster(Puyo.GREEN, Puyo.BLUE);
		Cluster nextPuyo = new Cluster(Puyo.PURPLE, Puyo.BLUE);

		PuyoState puyoState = invoke(ai, "detectPutState", arr, currentPuyo, nextPuyo);

		assertEquals(4, puyoState.getRow());
	}

}