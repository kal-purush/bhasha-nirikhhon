package csgfortridi;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JFrame;
import javax.swing.Timer;
import javax.swing.UIManager;

import csgfortridi.bsp.BSPnode;
import tridi.base.STransform;
import tridi.base.TriangleCoords;
import tridi.geom.CompoundGeometry;
import tridi.geom.FaceSet;
import tridi.render.SPanel;
import tridi.render.SViewer;
import tridi.render.Stage;
import tridi.render.Transformation;

/**
 * A test JFrame
 */
public class TestCSG extends JFrame {

	public TestCSG() {
		super("CSG for Tridi");
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		SViewer viewer = new SViewer(5.0, 5.0);
		final SPanel tp = viewer.spanel;
		tp.setPreferredSize(new Dimension(800, 600));
		getContentPane().add(viewer, BorderLayout.CENTER);
		Stage sc = tp.getStage();

		final Transformation<CompoundGeometry> cg = new Transformation<CompoundGeometry>(new CompoundGeometry());
		sc.addGeometry(cg);

		FaceSet fs = new FaceSet();
		fs.addPoint(0.0, 0.0, 0.0);
		fs.addPoint(1.0, 0.0, 0.0);
		fs.addPoint(0.0, 1.0, 0.0);
		fs.addPoint(1.0, 1.0, 0.0);
		fs.addPoint(0.0, 0.0, 1.0);
		fs.addPoint(1.0, 0.0, 1.0);
		fs.addPoint(0.0, 1.0, 1.0);
		// fs.addPoint(1.0, 1.0, 1.0);
		fs.addPoint(0.75, 0.75, 0.75);
		fs.addTriangle(0, 1, 5);
		fs.addTriangle(0, 5, 4);
		fs.addTriangle(1, 3, 7);
		fs.addTriangle(1, 7, 5);
		fs.addTriangle(4, 5, 6);
		fs.addTriangle(5, 7, 6);
		fs.addTriangle(0, 2, 1);
		fs.addTriangle(1, 2, 3);
		fs.addTriangle(7, 3, 2);
		fs.addTriangle(7, 2, 6);
		fs.addTriangle(0, 4, 2);
		fs.addTriangle(4, 6, 2);

		fs.calcNormals();
		for (TriangleCoords tr : fs.triangles) {
			System.out.println(tr.normals[0]);
		}

		BSPnode brickBSP = new BSPnode(fs);
		System.out.println(BSPnode.numberOfNodes + " Knoten erzeugt.");
		System.out.println(BSPnode.numberOfPolys + " Polygone gespeichert.");
		cg.geometry.add(fs);
		// cg.geometry.add(new Brick(new SPoint(0.0, 0.0, 0.0), new SVector(1.0, 1.0,
		// 1.0)));
		brickBSP.print("");

		cg.transform = new STransform();
		timer = new Timer(100, new ActionListener() {
			private double angle = 0.0;

			@Override
			public void actionPerformed(final ActionEvent e) {
				// angle+=Math.PI / 100.0;
				if (angle >= Math.PI) {
					angle -= 2.0 * Math.PI;
				}
				cg.transform.setToIdentity();
				cg.transform.rotate(0, angle);
				cg.transform.rotate(1, 2.0 * angle);
				tp.render();
				tp.repaint();
			}
		});
	}

	private final Timer timer;

	@Override
	public void dispose() {
		if (timer != null) {
			timer.stop();
		}
		super.dispose();
	}

	public static void main(final String[] args) {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception x) {
			x.printStackTrace();
		}
		TestCSG t = new TestCSG() {
			@Override
			public void dispose() {
				super.dispose();
				System.exit(0);
			}
		};
		t.pack();
		t.setVisible(true);
		if (t.timer != null) {
			t.timer.start();
		}
	}
}