package application;

import javafx.scene.paint.Color;

public class Constants {
	
	public static final int TITLE_BAR_HEIGHT = 50;
	
	public static final int BLOCK_AMOUNT = 51;
	public static final int BLOCK_SIZE = 10;
	public static final int BORDER_WIDTH = 1;
	public static final int HANDLE_LENGTH = 20;
	
	public static final String BUTTON_ICON_SIZE = "15px";
	
//	Blocks combined width + borders combined width
	public static final int PREF_CANVAS_SIZE = BLOCK_SIZE * BLOCK_AMOUNT + ((BLOCK_AMOUNT - 1) * BORDER_WIDTH);
//	Blocks combined width + borders combined width + handles length
	public static final int PREF_EDITOR_CANVAS_SIZE = BLOCK_SIZE * BLOCK_AMOUNT + ((BLOCK_AMOUNT - 1) * BORDER_WIDTH) + HANDLE_LENGTH;
	
//	Colors for painting
	public static final Color LIVE_CELL_COLOR = Color.ORANGERED;
	public static final Color DEAD_CELL_COLOR = Color.web("#222222");
	public static final Color INACTIVE_CELL_COLOR = Color.web("#090909");
	public static final Color ACTIVE_HANDLE_COLOR = Color.web("#22FF22");
	public static final Color INACTIVE_HANDLE_COLOR = Color.web("#FF2222");
	
//	Spacing between components
	public static final int SPACING = 5;
}