import java.awt.*;
import javax.swing.*;
import javax.swing.border.*;
import java.awt.event.ActionEvent;  
import java.awt.event.ActionListener;
import java.awt.event.MouseListener;
import java.io.*;
import java.util.*;
import java.util.Random;
import java.awt.Desktop;

public class gui  {
	static JFrame frame= new JFrame();
	static int max_str=1024;
	static Graph G = new Graph();
	
	public static void main(String[] args) {
		gui gui =new gui();
		gui.start();
	}
	
	public void start() {
		
		GUIPanel panel= new GUIPanel();
		frame.add(panel);
		frame.setTitle("GUI PANEL");
		frame.setSize(800, 600);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);
        Dimension d = Toolkit.getDefaultToolkit().getScreenSize();  
        int width = 800;  
        int height = 600;  
        frame.setBounds((d.width - width) / 2, (d.height - height) / 2, width, height);  
		
	}
    static int Is_exist(Graph G, int v1, int v2) {
        EdgeNode p = new EdgeNode();
        p = G.node_list[v1].first_edge;

        while (p != null) {
            if (p.vertex == v2) {
                p.weight++;

                return 1;
            }

            p = p.next;
        }

        return -1;
    }
    static String str1 = "";
    static String[] count ;
    static void creatGraph(Graph G, String[] str_all) {
        G.vertex_num = 0;

        HashSet hset = new HashSet(Arrays.asList(str_all));
        Iterator lt = hset.iterator();

        while (lt.hasNext()) {
            G.node_list[G.vertex_num] = new VertexNode();
            G.node_list[G.vertex_num].data = (lt.next()).toString();
            G.node_list[G.vertex_num].first_edge = null;
            G.vertex_num++;
        }

        for (int i = 0; i < (str_all.length - 1); i++) {
            EdgeNode p = new EdgeNode();
            int v1 = locate_vex(G, str_all[i]);
            int v2 = locate_vex(G, str_all[i + 1]);

            if (Is_exist(G, v1, v2) == -1) {
                p.vertex = v2;
                p.weight += 1;
                p.next = G.node_list[v1].first_edge;
                G.node_list[v1].num_edge++;
                G.node_list[v1].first_edge = p;
            } else {
                continue;
            }
        }
    }

    static public int locate_vex(Graph G, String s) {
        for (int i = 0; i < G.vertex_num; i++) {
            if (G.node_list[i].data.equals(s)) {
                return i;
            }
        }

        return -1;
    }

    static void print_it(Graph G) {
        EdgeNode p = new EdgeNode();

        for (int i = 0; i < G.vertex_num; i++) {
            System.out.print(G.node_list[i].data);

            p = G.node_list[i].first_edge;

            while (p != null) {
                System.out.print("->" + G.node_list[p.vertex].data);
                p = p.next;
            }

            System.out.println();
        }
    }

    static public String[] findBridgeWords(Graph G, String word1, String word2,
        int[] num_word) {
        int locate_word1 = locate_vex(G, word1);
        String[] tmp = new String[G.vertex_num];
        String s = "";
        EdgeNode p = new EdgeNode();
        EdgeNode q = new EdgeNode();
        p = G.node_list[locate_word1].first_edge;

        while (p != null) {
            int locate_1 = locate_vex(G, G.node_list[p.vertex].data);
            q = G.node_list[locate_1].first_edge;

            while (q != null) {
                if (G.node_list[q.vertex].data.equals(word2)) {
                    tmp[num_word[0]++] = G.node_list[p.vertex].data;
                }

                q = q.next;
            }

            p = p.next;
        }

        return tmp;
    }

    static public String queryBridgeWords(Graph G, String word1, String word2) {
        int locate_word1 = locate_vex(G, word1);
        int locate_word2 = locate_vex(G, word2);

        if ((locate_word1 == -1) || (locate_word2 == -1)) {
            if ((locate_word1 == -1) && (locate_word2 != -1)) {
                return "No " + word1 + " in the graph!";
            } else if ((locate_word2 == -1) && (locate_word1 != -1)) {
                return "No " + word1 + " in the graph!";
            } else {
                return "No " + word1 + " and " + word2 + " in the graph!";
            }
        } else {
            int[] num_word = { 0 };
            String[] tmp = findBridgeWords(G, word1, word2, num_word);
            String s = "";

            if (num_word[0] == 0) {
                return "No bridge words from word1 to word2!";
            } else if (num_word[0] == 1) {
                return "The bridge words from word1 to word2 are: " + tmp[0];
            } else {
                for (int i = 0; i < (num_word[0] - 1); i++)
                {
                    s+= tmp[i] + ", ";
                }

                return "The bridge words from word1 to word2 are: " + s +
                "and " + tmp[num_word[0] - 1] + ". ";
            }
        }
    }
    static void DrawGraph(Graph G) {
        EdgeNode p = new EdgeNode();
		GraphViz gViz=new GraphViz( "F:\\software\\Coding\\JAVA","C:\\Program Files (x86)\\Graphviz2.38\\bin\\dot.exe");
        gViz.start_graph();
        for (int i = 0; i < G.vertex_num; i++) {

            p = G.node_list[i].first_edge;

            while (p != null) {
				gViz.addln(G.node_list[i].data +"->"+G.node_list[p.vertex].data+";");
                p = p.next;
            }
        }
        gViz.end_graph();
        try {
            gViz.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String generateNewText(Graph G, String inputText) {
        int[] arrays_length = { 0 };
        int num = 0;
        Random random = new Random();
        String s = "";
        String[] tmp = new String[G.vertex_num];
        inputText= (inputText.replaceAll("[^A-Za-z., \n?!:;(){}\"\'-]","").replaceAll("[^A-Za-z]"," ").replaceAll(" +"," ")).trim();
        inputText = inputText.toLowerCase();
        String[] inputText_split = (inputText.toLowerCase()).split("\\s+");
        num = inputText_split.length;

        for (int i = 0; i < num-1; i++) {
            s += (inputText_split[i] + " ");

            if (locate_vex(G, inputText_split[i]) == -1) {
                continue;
            } else {
                tmp = findBridgeWords(G, inputText_split[i],
                        inputText_split[i + 1], arrays_length);

                if (arrays_length[0] != 0) {
                    s += (tmp[0] + " ");
                    arrays_length[0] = 0;
                }
            }
        }
        s += (inputText_split[inputText_split.length-1] + " ");
        return s;
    }

    public static String randomWalk(Graph G) {
        Random random = new Random();
        int num = random.nextInt(G.vertex_num);
        int rand_num = 0 ,pre=num;
        String s = G.node_list[num].data + " ";
        String s1 = s;
        EdgeNode p = new EdgeNode();
        EdgeNode q = new EdgeNode();
        p=G.node_list[num].first_edge;
        while (true ) 
        {
            if (p == null || p.flag==1)
             { 
                if(p==null)
                {
                      break;
                }
                else
                {
                    s += G.node_list[p.vertex].data;
                     break;
                }
            }
            else
            {
                rand_num = random.nextInt((G.node_list[pre]).num_edge);
                for (int i = 0; i < rand_num; i++)
                {
                    p = p.next;
                }
                pre=p.vertex;
                p.flag = 1;
                s += (G.node_list[p.vertex].data + " ");
                num = G.node_list[p.vertex].num_edge;
                p = G.node_list[p.vertex].first_edge;

            }
        }
        for (int i = 0; i < G.vertex_num; i++) {
            q = G.node_list[i].first_edge;
            while (q != null) {
                q.flag=0;
                q = q.next;
            }
        }
        try{
          File f = new File("F:\\software\\Coding\\JAVA\\rewrite_txt.txt");
          FileOutputStream fos = new FileOutputStream(f);  
          if(!f.exists())
          {
            f.createNewFile();
          }
           byte[] contentInBytes = s.getBytes();
            fos.write(contentInBytes);
            fos.flush();
            fos.close();
      }catch(IOException e)
      {
        e.printStackTrace();
      }
        return s;
    }


    public static String calcShortestPath(Graph G, String word1, String word2) {
        int max_num = 99;
        String s="";
        int[][] matraix = new int[G.vertex_num][G.vertex_num];
        int[][] tmp_matraix = new int[G.vertex_num + 1][G.vertex_num + 1];

        for (int i = 0; i < G.vertex_num; i++)
            for (int j = 0; j < G.vertex_num; j++) {
                if (i == j) {
                    matraix[i][j] = 0;
                } else {
                    matraix[i][j] = max_num;
                }
            }

        EdgeNode p = new EdgeNode();

        for (int i = 0; i < G.vertex_num; i++) {
            p = G.node_list[i].first_edge;

            while (p != null) {
                matraix[i][p.vertex] = p.weight;
                p = p.next;
            }
        }

        for (int i = 0; i < G.vertex_num; i++)
            for (int j = 0; j < G.vertex_num; j++)
                if (matraix[i][j] != max_num) {
                    tmp_matraix[i][j] = j;
                }


        int[] Line = new int[G.vertex_num];
        int v1 = locate_vex(G, word1);
        int v2 = locate_vex(G, word2);
        if ((v1 == -1) || (v2 == -1)) {
            if ((v1 == -1) && (v2 != -1)) {
                return "No " + word1 + " in the graph!";
            } else if ((v2 == -1) && (v1 != -1)) {
                return "No " + word1 + " in the graph!";
            } else {
                return "No " + word1 + " and " + word2 + " in the graph!";
            }
        }
        for (int k = 0; k < G.vertex_num; ++k) {
            for (int i = 0; i < G.vertex_num; ++i) {
                for (int j = 0; j < G.vertex_num; ++j) {
                    if ((matraix[i][k] + matraix[k][j]) < matraix[i][j]) {
                        matraix[i][j] = matraix[i][k] + matraix[k][j];
                        tmp_matraix[i][j] = tmp_matraix[i][k];
                    }
                }
            }
        }
        int next = tmp_matraix[v1][v2];
        int num=matraix[v1][v2];
        String ss="";
        if (next == 0) {
            return "Unreachable!";
        } else {
            s+=G.node_list[v1].data + " ->";
            ss+=G.node_list[v1].data+" ";
            while (next != v2 ) {
                s+=G.node_list[next].data + " ->";
                ss+=G.node_list[next].data +" ";
                next = tmp_matraix[next][v2];
            }
            s+=G.node_list[v2].data ;
            ss+=G.node_list[v2].data;
            String []s_tmp=ss.split("\\s+");
            set_graph(G,s_tmp,num);
        }

        return s;
    }
    public static void set_graph(Graph G,String []str_tmp,int num)
    {
        EdgeNode p=new EdgeNode();
        GraphViz gViz=new GraphViz( "F:\\software\\Coding\\JAVA","C:\\Program Files (x86)\\Graphviz2.38\\bin\\dot.exe","temp");
        gViz.start_graph();
        gViz.addln(str_tmp[0]+"->"+str_tmp[str_tmp.length-1]+" [color=blue] "+"[label="+num+"]"+";");
        for(int i=0;i<str_tmp.length-1;i++)
        {
            gViz.addln(str_tmp[i]+"->"+str_tmp[i+1]+" [color=red]"+";");
            
        }
        for (int i = 0; i < G.vertex_num; i++) {
            p = G.node_list[i].first_edge;
            while (p != null) {
                gViz.addln(G.node_list[i].data +"->"+G.node_list[p.vertex].data+";");
                p = p.next;
            }
        }
        gViz.end_graph();
        try {
            gViz.run();
        } catch (Exception e) {
            e.printStackTrace();
      
    }
    }
}
class VertexNode {
    String data;
    int num_edge;
    EdgeNode first_edge = new EdgeNode();
}

class EdgeNode {
    int vertex;
    int weight;
    int flag;
    EdgeNode next;
}

class Graph {
    VertexNode[] node_list = new VertexNode[1000];
    int vertex_num;
}

class GUIPanel extends JPanel implements ActionListener{ 
	static JLabel lable1=new JLabel("current file path:null");
	static JTextArea out1 = new JTextArea();
    static JTextArea out2 = new JTextArea();
	static JButton btnload= new JButton("load the graph");
	static JButton btnselect= new JButton("select");
	static JButton btnsave= new JButton("Save graph as..");
	static JPanel ptop = new JPanel(new GridLayout(3,2,5,5));	
	static JPanel pbtn = new JPanel(new GridLayout(1,2,5,5));	
	static JLabel pimage =new JLabel();
	static JButton btn21= new JButton("Show & Refresh generated graph");
	static JButton btn22= new JButton("Open graph in system image viewer");
	static JButton btn23= new JButton("Query term bridging words");
	static JButton btn24= new JButton("Generate new text based on bridge word");
	static JButton btn25= new JButton("Calculate the shortest path between two words");
	static JButton btn26= new JButton("Random walk");
    static JButton btn27= new JButton("Continue?");
    static JButton btn28= new JButton("No!"); 
	static JPanel p5 = new JPanel();
    static JFrame jf=new JFrame();          
    static Panel  p=new Panel();
    int counter=0;
    String save_random="";
	public GUIPanel() {  
		this.setLayout(new GridLayout(2,2,7,7));  
		JPanel p1 = new JPanel();	
		JPanel p2 = new JPanel(new GridLayout(2,4,5,5));
		JPanel p3 = new JPanel();
		JPanel p4 = new JPanel();

		pbtn.add(p4);
		pbtn.add(p5);
		ptop.add(p1);
		ptop.add(p3);
		ptop.add(p2);	
		this.add(ptop);
		this.add(pbtn);
		btnload.addActionListener(this);  
		btnselect.addActionListener(this); 
		btnsave.addActionListener(this); 
		btn21.addActionListener(this);  
		btn22.addActionListener(this); 
		btn23.addActionListener(this);  
		btn24.addActionListener(this); 
		btn25.addActionListener(this);  
		btn26.addActionListener(this);
        btn27.addActionListener(this);
        btn28.addActionListener(this); 
		p1.add(lable1);
		p1.add(btnselect);
		p1.add(btnload);		
		p1.setBorder(new TitledBorder("File Input"));	
		p2.setBorder(new TitledBorder("Oprations"));


		p2.add(btn23);
		p2.add(btn24);
		p2.add(btn25);
		p2.add(btn26);		
		
		p3.setBorder(new TitledBorder("Graph Oprations"));
		p3.add(btn21);
		p3.add(btn22);
		p3.add(btnsave);

        Dimension a = new Dimension();
		jf.setSize(500,500);
        jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        a.setSize(490,300);
        out2.setText("");
        out2.setPreferredSize(a);
        out2.setAutoscrolls(true);
        out2.setLineWrap(true);   
        p.add(out2);
        p.add(btn27,BorderLayout.SOUTH);
        p.add(btn28,BorderLayout.SOUTH);
        jf.add(p);

		Dimension aa = new Dimension();
		aa.setSize(350,230);
		out1.setPreferredSize(aa);
		out1.setText("result here");
		out1.setAutoscrolls(true);
		out1.setLineWrap(true);
		p4.add(out1);
		p4.setBorder(new TitledBorder("Result"));		
		p5.setSize(400,300);
		p5.setBorder(new TitledBorder("Graph"));
		JScrollPane scrollPane=new JScrollPane(GUIPanel.pimage);
		p5.add(scrollPane);
		scrollPane.setPreferredSize(new Dimension(350,240));
		scrollPane.setAutoscrolls(true);
		scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
		scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
	};

    public void actionPerformed(ActionEvent event) {
    	Object source = event.getSource();   
    	if(source == btnselect) { 
	        FileDialog d = new FileDialog(gui.frame,"select Input file");
	        d.show(); 
	        String openFile; 
	        
	        if((openFile = d.getFile()) != null){ 
	        	GUIPanel.lable1.setText(d.getDirectory()+openFile); 
	        	
	        } 
    	}
    	if(source == btnload) {
            try {
                File f = new File(GUIPanel.lable1.getText());
                //MaxSize
                if (f.length()>0 && f.length()<65536){
                	FileInputStream fis = new FileInputStream(f);
                    
                    byte[] b = new byte[(int) f.length()];
                    fis.read(b);
                    fis.close();
                    gui.str1="";
                    for (int i = 0; i < f.length(); i++) {
                        gui.str1 += (char) b[i];
                    }
                    gui.str1= (gui.str1.replaceAll("[^A-Za-z., \n?!:;(){}\"\'-]","").replaceAll("[^A-Za-z]"," ").replaceAll(" +"," ")).trim();
                    gui.str1 = gui.str1.toLowerCase();
                    GUIPanel.out1.setText(gui.str1); 
                    gui.count = (gui.str1).split("\\s+");
                    gui.creatGraph(gui.G, gui.count);
            		JOptionPane.showMessageDialog(gui.frame,"Load Done");
                	
                }
                else {
                	JOptionPane.showMessageDialog(gui.frame,"Invivid File!");
                }
                

            } catch (IOException e) {
                e.printStackTrace();
    	}
      } 
    	/*
		21.Read the processed text and generate the graph");
		22.Show  directed graphs");
		23.Query term bridging words");
		24.Generate new text based on bridge word");
		25.Calculate the shortest path between two words");
		26.Random walk");
		*/
    	if(source == btnsave) {
    		FileDialog d = new FileDialog(gui.frame,"select place to save");
    		
	        d.show(); 
	        
	        String openFile = d.getDirectory(); 
	        try{
	        	File temp = new File("temp.gif");
	        	if(temp.exists()) {
	        		FileInputStream fis = new FileInputStream(temp); 
	        		byte[] b = new byte[(int) temp.length()];
	                fis.read(b);
	                String s="";
	                for (int i = 0; i < temp.length(); i++) {
	                    s += (char) b[i];
	                }
		            File f = new File(openFile+"output.gif");
		            FileOutputStream fos = new FileOutputStream(f);  
		            if(!f.exists())
		            {
		              f.createNewFile();
		            }
		             byte[] contentInBytes = s.getBytes();
		              fos.write(contentInBytes);
		              fos.flush();
		              fos.close();}
	        }catch(IOException e)
	        {
	          e.printStackTrace();
	        }
	        
	        
    	}
    	
    	
    	if(source == btn21) {
    		
    		File srcFile = new File(".\\temp.gif");
    		byte b[]=new byte[(int)srcFile.length()]; 
    		InputStream in;
			try {
				in = new FileInputStream(srcFile);
				try {
					in.read(b);
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
	    	
    		
    		ImageIcon icon = new ImageIcon(b);
    		GUIPanel.pimage.setIcon(icon);
    		gui.DrawGraph(gui.G);
    		
    	}
    	if(source == btn22) {
    		File file=new File(".\\temp.gif");
    		Desktop desk= Desktop.getDesktop();
    		try {
				desk.open(file);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	
    				
    	}
    	if(source == btn23) {
    		String Result1 = JOptionPane.showInputDialog(gui.frame, "Input word 1:");
    		String Result2 = JOptionPane.showInputDialog(gui.frame, "Input word 2:");
    		String tmp = gui.queryBridgeWords(gui.G,Result1,Result2);
    		
    		GUIPanel.out1.setText(tmp); 
    		JOptionPane.showMessageDialog(gui.frame,GUIPanel.out1.getText());
    		
    	}
    	if(source == btn24) {
    		String Result1 = JOptionPane.showInputDialog(gui.frame, "Input word:");
    		GUIPanel.out1.setText(gui.generateNewText(gui.G,Result1));
    		JOptionPane.showMessageDialog(gui.frame,GUIPanel.out1.getText());
    	}
    	if(source == btn25) {
    		String Result1 = JOptionPane.showInputDialog(gui.frame, "Input word 1:");
    		String Result2 = JOptionPane.showInputDialog(gui.frame, "Input word 2:");
    		String tmp = gui.calcShortestPath(gui.G,Result1,Result2);
    		GUIPanel.out1.setText(tmp); 
    		JOptionPane.showMessageDialog(gui.frame,"The calcShortestPath path is: "+GUIPanel.out1.getText());
    	}
    	if(source == btn26 || source==btn27 || source==btn28) {
            if(source==btn26)
            {
                GUIPanel.out2.setText("");
                String random_path=gui.randomWalk(gui.G);
                Dimension d = Toolkit.getDefaultToolkit().getScreenSize();  
                int width = 500;  
                int height = 500;  
                 jf.setBounds((d.width - width) / 2, (d.height - height) / 2, width, height);
                save_random=random_path;
                jf.show();
                counter=0;
                
            }
            else if(source==btn27)
            {
                String []temp_random=save_random.split(" ");
                String temp_rand="";
                if(counter==temp_random.length)
                {       
                     JOptionPane.showMessageDialog(gui.frame,"That's all","warning",JOptionPane.INFORMATION_MESSAGE);
                }
                else
                {
                    for(int i=0;i<=counter;i++)
                    {
                        temp_rand+=temp_random[i]+" ";
                     }
                    counter++;
                     GUIPanel.out2.setText(temp_rand);
                }
    	   }
           else
           {
            String []temp_random=save_random.split(" ");
            String temp_rand="";
            for(int i=0;i<counter;i++)
            {
                temp_rand+=temp_random[i]+" ";
            }
                     jf.dispose();
                    GUIPanel.out1.setText(temp_rand);
           }
        }

}
}
class  GraphViz{
    private String runPath = "";
    private String dotPath = ""; 
    private String runOrder="";
    private String dotCodeFile="dotcode.txt";
    private String resultGif="temp";
    private StringBuilder graph = new StringBuilder();

    Runtime runtime=Runtime.getRuntime();

    public void run() {
        File file=new File(runPath);
        file.mkdirs();
        writeGraphToFile(graph.toString(), runPath);
        creatOrder();
        try {
            runtime.exec(runOrder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void creatOrder(){
        runOrder+=dotPath+" ";
        runOrder+=runPath;
        runOrder+="\\"+dotCodeFile+" ";
        runOrder+="-T gif ";
        runOrder+="-o ";
        runOrder+=runPath;
        runOrder+="\\"+resultGif+".gif";
        System.out.println(runOrder);
    }

    public void writeGraphToFile(String dotcode, String filename) {
        try {
            File file = new File(filename+"\\"+dotCodeFile);
            if(!file.exists()){
                file.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(dotcode.getBytes());
            fos.close();
        } catch (java.io.IOException ioe) { 
            ioe.printStackTrace();
        }
     }  

    public GraphViz(String runPath,String dotPath,String nameGif) {
        this.runPath=runPath;
        this.dotPath=dotPath;
		this.resultGif=nameGif;
    }
    public GraphViz(String runPath,String dotPath) {
        this.runPath=runPath;
        this.dotPath=dotPath;
    }
    public void add(String line) {
        graph.append("\t"+line);
    }

    public void addln(String line) {
        graph.append("\t"+line + "\n");
    }

    public void addln() {
        graph.append('\n');
    }

    public void start_graph() {
        graph.append("digraph G {\n") ;
    }

    public void end_graph() {
        graph.append("}") ;
    }   
}


	
