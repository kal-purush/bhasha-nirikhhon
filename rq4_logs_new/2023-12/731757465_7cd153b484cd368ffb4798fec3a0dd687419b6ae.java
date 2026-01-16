package FinalProject;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import javax.media.j3d.*;
import javax.vecmath.*;
import com.sun.j3d.utils.behaviors.mouse.*;
import com.sun.j3d.utils.geometry.Sphere;
import com.sun.j3d.utils.universe.SimpleUniverse;

public class ProteinVisualizer extends JFrame {
    /**
	 * 
	 */
	private static final long serialVersionUID = -1430715696852347163L;
	private JButton uploadButton;
    private JFileChooser fileChooser;
    private Protein3DViewer proteinViewer;
    private JTextField searchField;

    public ProteinVisualizer() {
        super("Protein Sequence Visualization");
        setSize(800, 600);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        JPanel panel = new JPanel(new BorderLayout());
        uploadButton = new JButton("Upload PDB File");
        fileChooser = new JFileChooser();

        proteinViewer = new Protein3DViewer();
        panel.add(uploadButton, BorderLayout.NORTH);
        panel.add(proteinViewer, BorderLayout.CENTER);

        JPanel searchPanel = new JPanel(new FlowLayout());
        searchField = new JTextField(20);
        JButton searchButton = new JButton("Search");
        searchPanel.add(new JLabel("Search:"));
        searchPanel.add(searchField);
        searchPanel.add(searchButton);
        panel.add(searchPanel, BorderLayout.SOUTH);

        add(panel);

        uploadButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                int returnValue = fileChooser.showOpenDialog(ProteinVisualizer.this);
                if (returnValue == JFileChooser.APPROVE_OPTION) {
                    File selectedFile = fileChooser.getSelectedFile();
                    if (validatePDBFile(selectedFile)) {
                        proteinViewer.loadProteinStructure(selectedFile);
                    } else {
                        JOptionPane.showMessageDialog(ProteinVisualizer.this, "Please select a valid PDB file.");
                    }
                }
            }
        });

        searchButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String query = searchField.getText();
                proteinViewer.searchAminoAcid(query);
            }
        });
    }

    private boolean validatePDBFile(File file) {
        return file != null && file.getName().toLowerCase().endsWith(".pdb");
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                new ProteinVisualizer().setVisible(true);
            }
        });
    }

    class Protein3DViewer extends Canvas3D {
        /**
		 * 
		 */
		private static final long serialVersionUID = -8578261456831996686L;
		/**
		 * 
		 */

		private TransformGroup objRotate;

        public Protein3DViewer() {
            super(SimpleUniverse.getPreferredConfiguration());

            SimpleUniverse universe = new SimpleUniverse(this);
            universe.getViewingPlatform().setNominalViewingTransform();

            BranchGroup scene = createSceneGraph();
            scene.compile();

            universe.addBranchGraph(scene);
        }

        private BranchGroup createSceneGraph() {
            BranchGroup objRoot = new BranchGroup();

            objRotate = new TransformGroup();
            objRotate.setCapability(TransformGroup.ALLOW_TRANSFORM_WRITE);
            objRotate.setCapability(TransformGroup.ALLOW_TRANSFORM_READ);

            setupMouseControls(objRotate, objRoot);

            // Add a simple sphere to the scene (example)
            addSphereToScene(objRotate);

            objRoot.addChild(objRotate);
            return objRoot;
        }

        private void setupMouseControls(TransformGroup objRotate, BranchGroup objRoot) {
            MouseRotate myMouseRotate = new MouseRotate();
            myMouseRotate.setTransformGroup(objRotate);
            myMouseRotate.setSchedulingBounds(new BoundingSphere());
            objRoot.addChild(myMouseRotate);

            MouseZoom myMouseZoom = new MouseZoom();
            myMouseZoom.setTransformGroup(objRotate);
            myMouseZoom.setSchedulingBounds(new BoundingSphere());
            objRoot.addChild(myMouseZoom);
        }

        private void addSphereToScene(TransformGroup objRotate) { // Used external help to set-up this thread
            Color3f color = new Color3f(1.0f, 0.0f, 0.0f); // Red color 
            Sphere sphere = new Sphere(0.05f); //  
            Appearance appearance = new Appearance();
            ColoringAttributes ca = new ColoringAttributes(color, ColoringAttributes.SHADE_FLAT);
            appearance.setColoringAttributes(ca);
            sphere.setAppearance(appearance);

            TransformGroup tg = new TransformGroup();
            Transform3D transform = new Transform3D();
            Vector3f vector = new Vector3f(.0f, .0f, .0f);
            transform.setTranslation(vector);
            tg.setTransform(transform);
            tg.addChild(sphere);
            objRotate.addChild(tg);
        }

        public void loadProteinStructure(File pdbFile) {
            // Implement the logic to load and display the 3D structure
            System.out.println("Loading protein structure from: " + pdbFile.getAbsolutePath());
        }

        public void searchAminoAcid(String query) {
            // Implement the search logic and visually update the 3D model
            System.out.println("Searching for: " + query);
        }
    }
}