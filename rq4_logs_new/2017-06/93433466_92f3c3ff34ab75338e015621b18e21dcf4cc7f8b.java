import javax.swing.JFrame;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.view.mxGraph;

public class JGraphXFrame extends JFrame{
	
	public JGraphXFrame(Graph newGraph){
		super("JGraphXFrame");
	    mxGraph graph = new mxGraph();
	    Object defaultParent = graph.getDefaultParent();
	    graph.getModel().beginUpdate();
	    
	    /*
	     * THIS WORK:
	    Object v1 = graph.insertVertex(defaultParent, null, "Hello", 20, 20, 80, 30);
	    Object v2 = graph.insertVertex(defaultParent, null, "World", 240, 150, 80, 30);
	    graph.insertEdge(defaultParent, null, "Edge", v1, v2);
	    
	    */
	    
	    //plotting the nodes
	    int x = 0, y = 0;
	    for(int aux=0; aux<newGraph.getNodeList().size(); aux++){
	    	String currentNodeName = new String();
	    	currentNodeName = newGraph.getNodeList().get(aux).getName();
	    	Object plottedNode = graph.insertVertex(defaultParent, null,currentNodeName, 20*x, 20*y, 130, 30);
	    	
	    	y = y +3;
	    	if (aux%5 == 4){
	    		x = x+10;
	    		y = 0;
	    	}
	    	
	    }
	    
	    //TODO: Plot the edges
	    
	    
	    graph.getModel().endUpdate();
	    mxGraphComponent graphComponent = new mxGraphComponent(graph);
	    getContentPane().add(graphComponent);

	    //attributes to edit the graph during execution
	    graph.setCellsEditable(false);
	    graph.setCellsMovable(true);  
	    graph.setCellsResizable(false);
	    graph.setCellsSelectable(true); 
	    graph.setEnabled(true); 
	     
	    graphComponent.setConnectable(false); // Inhibit edge creation in the graph.
	}

	public static void createFrame(Graph newGraph) {
		JGraphXFrame frame = new JGraphXFrame(newGraph);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(400, 320);
		frame.setVisible(true);
		frame.setLocationRelativeTo(null);
	}
}