import game.GameBoard;
import renderer.SwingRenderer;

public class Application
{
    public static void main(String[] args)
    {
        GameBoard gameBoard = new GameBoard();
        SwingRenderer renderer = new SwingRenderer(gameBoard);
    }
}