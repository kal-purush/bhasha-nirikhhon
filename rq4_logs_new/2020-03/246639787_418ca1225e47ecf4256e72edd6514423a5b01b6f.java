import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.util.ArrayList;

import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

public class BarChartSkeleton extends JPanel {

	public static final int Y_GRID_BUFFER = 30;
	public static final int Y_GRID_FACTOR = 8;
	public static final int Y_GRID_SCALE = 5;

	public static final int X_GRID_FACTOR = 60;
	public static final int X_GRID_BUFFER = 40;
	
	public static final int MAX_VALUE = 50;
	public static final int MIN_VALUE = -20;
	
	private BarChart data;
	
	public BarChartSkeleton() {
		super(new BorderLayout());
		data = new BarChart();
		data.setOpaque(false);
		this.add(data, BorderLayout.CENTER);
	}
	
	public void setBars(ArrayList<BarData> data)
	{
		this.data.setBars(data);
	}
	
	public void setTitle(String title) {
		this.data.setTitle(title);
	}
		

	@Override
	protected void paintComponent(Graphics g)
	{
		super.paintComponent(g);
		
		this.setBackground(Color.WHITE);
		g.setColor(getBackground());
		g.fillRect(0, 0, getWidth(), getHeight());
		g.setColor(getForeground());
				
		for(int i=0; i <= MAX_VALUE - MIN_VALUE; i++) {
			if(i%Y_GRID_SCALE == 0) {
				int y = getHeight() - i*Y_GRID_FACTOR - Y_GRID_BUFFER;
				int x = 10;
				int line = i + MIN_VALUE;
				
				g.drawString(Integer.toString(line), x, y);
				g.drawLine(x + 20, y, getWidth() - x, y);
			}
		}
		
		for(int j = 12; j > 0; j--) {
			int y = getHeight() - 10;
			int x = getWidth() - (12-j)*X_GRID_FACTOR - X_GRID_FACTOR/2 - X_GRID_BUFFER;
			
			g.drawString(Integer.toString(j), x, y);
		}
	}

	@Override
	public Dimension getPreferredSize() {
		return new Dimension(800, 700);
	}

}