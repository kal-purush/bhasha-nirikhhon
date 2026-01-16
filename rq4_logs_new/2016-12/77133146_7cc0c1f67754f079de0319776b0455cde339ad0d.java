package ex11Q2;

import java.awt.Color;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Insets;

import javax.swing.JPanel;

/**
 * Represents the graphical display of the board of the 2-D automaton.
 * @author Yaniv Tzur
 */
public class BoardView extends JPanel
{

	/**
	 * Needed for extending JPanel.
	 */
	private static final long serialVersionUID = 1942482025238702421L;
	
	private static final double IMAGE_SCALE_FACTOR = 0.4; // Used for scaling the size of drawn
														  // images.
	private static final double IMAGE_DRAWING_OFFSET = 0.3; // Used for determining the coordinates
															 // at which to draw an image.
	
	private int _boardCellSideLength; // The length of one side of a square board cell.
	private int _imageSize; // The size to use for each image drawn inside a cell of the automaton's
							// board.
	private int _width; // The width of the graphical display of the CA's board.
	private int _height; // The height of the graphical display of the CA's board.
	private State[][] _states; // The states of each cell of the CA.
	private Image _transparentFemaleImage; // Represents a female.
	private Image _transparentMaleImage; // Represents a male.
	private Image _transparentMaleFemaleCoupleImage; // Represents a couple.
	
	/**
	 * Creates the section of the window displaying the state of each cell of the CA.
	 * @param femaleImage The image of a "female" state.
	 * @param maleImage The image of a "male" state.
	 * @param heartImage Used in representing a couple.
	 * @param maleFemaleCoupleImage The image of a "male-female couple" state.
	 */
	public BoardView(Image femaleImage, Image maleImage,
			   		 Image maleFemaleCoupleImage)
	{
		_transparentFemaleImage = femaleImage;
		_transparentMaleImage = maleImage;
		_transparentMaleFemaleCoupleImage = maleFemaleCoupleImage;
		setBackground(Color.WHITE);
	}
	
	/**
	 * Updates the graphical display of the board of the automaton to show the current state
	 * of each cell.
	 * @param states The states of the cells of the automaton at the present generation.
	 */
	public void updateBoardDisplay(State[][] states)
	{
		int minDim = 0;
		Insets insets = null;
		Container currentComponent = this;
		_width = getWidth();
		_height = getHeight();
		while (currentComponent != null)
		{
			insets = currentComponent.getInsets();
			_width -= insets.right + insets.left;
			_height -= insets.top + insets.bottom;
			currentComponent = currentComponent.getParent();		
		}
		_states = states;
		minDim = (_width < _height) ? _width : _height;
		_boardCellSideLength = (int)((minDim / _states.length)*1);
		_imageSize = (int)(_boardCellSideLength * IMAGE_SCALE_FACTOR);
		repaint();
	}
	
	protected void paintComponent(Graphics g)
	{
		super.paintComponent(g);

		int cellStartX = 0;
		int cellStartY = 0;
		int imageStartX = 0;
		int imageStartY = 0;
		int stringStartX = 0;
		int stringStartY = 0;
		int imageDrawingOffset = (int)(_boardCellSideLength * IMAGE_DRAWING_OFFSET);
		State.StateType stateType = State.StateType.MAN;
		
		if (_states != null)
			for(int i = 0; i < _states.length; i++)
			{	
				cellStartX = i * _boardCellSideLength;
				imageStartX = cellStartX + imageDrawingOffset;
				stringStartX = imageStartX + _imageSize/4;
				for(int j = 0; j < _states.length; j ++)
				{
					cellStartY = j * _boardCellSideLength;
					imageStartY = cellStartY + imageDrawingOffset;
					stringStartY = imageStartY + _imageSize + 10;
					g.drawRect(cellStartX, cellStartY, _boardCellSideLength, _boardCellSideLength);
					if (_states[i][j] != null)
					{
						stateType = _states[i][j].getStateType();
						if (stateType == State.StateType.MAN)
							drawMan(g, imageStartX, imageStartY);
						else if (stateType == State.StateType.WOMAN)
							drawWoman(g, imageStartX, imageStartY);
						else if (stateType == State.StateType.COUPLE)
							drawCouple(g, imageStartX, imageStartY);
						g.drawString(Integer.toString(_states[i][j].getIndex(stateType)),
									 stringStartX,
									 stringStartY);
					}
				}
			}
	}
	
	/**
	 * Draws the image of a man in the middle of the cell.
	 */
	private void drawMan(Graphics g, int row, int col)
	{
		drawImage(g, _transparentMaleImage, row, col);
	}
	
	/**
	 * Draws the image of a woman in the middle of the cell.
	 */
	private void drawWoman(Graphics g, int row, int col)
	{
		drawImage(g, _transparentFemaleImage, row, col);
	}
	
	/**
	 * Draws the image of a woman in the middle of the cell.
	 */
	private void drawCouple(Graphics g, int row, int col)
	{
		drawImage(g, _transparentMaleFemaleCoupleImage, row, col);
	}
	
	/**
	 * Draws the input image on the input Graphics object with the input coordinates.
	 * @param g The object to draw the image onto.
	 * @param image The image to draw.
	 * @param row The X coordinate to start drawing the image at.
	 * @param col The Y coordinate to start drawing the image at.
	 */
	private void drawImage(Graphics g, Image image, int row, int col)
	{
		g.drawImage(image, row, col, _imageSize, _imageSize, null);
	}
}