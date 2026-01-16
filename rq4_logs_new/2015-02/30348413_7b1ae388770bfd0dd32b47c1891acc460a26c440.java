package fr.guillaumerose;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.InOrder;

public class MapTest {
	private final Robot robot1 = mock(Robot.class);
	private final Robot robot2 = mock(Robot.class);

	@Test
	public void should_execute_command_on_all_robots() throws Exception {
		when(robot1.hasNext()).thenReturn(true, true, false);
		when(robot2.hasNext()).thenReturn(true, false);
		when(robot1.summary()).thenReturn("Robot1: some position");
		when(robot2.summary()).thenReturn("Robot2: some position");

		Map map = new Map(newArrayList(robot1, robot2));
		map.run();

		verify(robot1, times(3)).hasNext();
		verify(robot2, times(2)).hasNext();

		InOrder order = inOrder(robot2);
		order.verify(robot2).hasNext();
		order.verify(robot2).next();
		order.verify(robot2).hasNext();
	}
}