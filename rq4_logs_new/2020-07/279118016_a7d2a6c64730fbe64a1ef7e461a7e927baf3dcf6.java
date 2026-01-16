import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Image;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

public class BackgroundImage extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	JScrollPane scrollPane;
	ImageIcon icon;
	Image image;

	public BackgroundImage() {
		this.icon = new ImageIcon("backGround.jpg");

		final JPanel panel = new JPanel() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			protected void paintComponent(Graphics g) {
				// Dispaly image at at full size
				g.drawImage(BackgroundImage.this.icon.getImage(), 0, 0, null);

				// Scale image to size of component
				// Dimension d = getSize();
				// g.drawImage(icon.getImage(), 0, 0, d.width, d.height, null);

				// Fix the image position in the scroll pane
				// Point p = scrollPane.getViewport().getViewPosition();
				// g.drawImage(icon.getImage(), p.x, p.y, null);

				super.paintComponent(g);
			}
		};
		panel.setOpaque(false);
		panel.setPreferredSize(new Dimension(400, 400));
		this.scrollPane = new JScrollPane(panel);
		this.getContentPane().add(this.scrollPane);

	}

	public static void main(final String[] args) {
		final BackgroundImage frame = new BackgroundImage();
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(300, 300);
		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
	}
}