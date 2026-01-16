import java.awt.Color;

public class Ball extends Sprite {
	private static final int BALL_WIDTH = 25;
	private static final int BALL_HEIGHT = 25;
	private static final Color BALL_COLOUR = Color.BLACK;

	public Ball(int panelWidth, int panelHeight) {
		setWidth(BALL_WIDTH);
		setHeight(BALL_HEIGHT);
		setColour(BALL_COLOUR);
		setInitialPosition(panelWidth / 2 - (getWidth() / 2), panelHeight / 2 - (getHeight() / 2));
		resetToInitialPosition();
	}
}	