package utils;

import graph.*;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;

public class DrawGraph extends JFrame {
    public DrawGraph(Graph graph, ArrayList<String> SP){
        int height = 600;
        double diffLat = graph.maxLat - graph.minLat;
        double diffLon = graph.maxLon - graph.minLon;

        JFrame frame = new JFrame();
        frame.add(new NewPanel(graph, height, (int) (height*diffLat/diffLon), SP));
        frame.setTitle("Map");

        frame.setSize((int) (height*diffLat/diffLon), height);

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }

}

class NewPanel extends JPanel{
    Graph graph;
    int height;
    int width;
    ArrayList<String> SP;
    public NewPanel(Graph g, int h, int w, ArrayList<String> SP){
        this.SP = SP;
        graph = g;
        height = h;
        width = w;
    }
    protected void paintComponent(Graphics g){
        super.paintComponent(g);
        for(List<Edges> eList : graph.getMyGraph().values()){
            for(Edges e : eList){
                int xLat = transPosition(e.getSrc().getstopLat(), graph.maxLat, graph.minLat, width);
                int xLon = transPosition(e.getSrc().getstopLon(), graph.maxLon, graph.minLon, height);
                int yLat = transPosition(e.getDst().getstopLat(), graph.maxLat, graph.minLat, width);
                int yLon = transPosition(e.getDst().getstopLon(), graph.maxLon, graph.minLon, height);

                g.drawLine(xLat, xLon, yLat, yLon);
            }

        }
        for(int i=0; i<SP.size()-1; i++){
            int xLat = transPosition(graph.getMyStops().get(SP.get(i)).getstopLat(), graph.maxLat, graph.minLat, width);
            int xLon = transPosition(graph.getMyStops().get(SP.get(i)).getstopLon(), graph.maxLon, graph.minLon, height);
            int yLat = transPosition(graph.getMyStops().get(SP.get(i+1)).getstopLat(), graph.maxLat, graph.minLat, width);
            int yLon = transPosition(graph.getMyStops().get(SP.get(i+1)).getstopLon(), graph.maxLon, graph.minLon, height);

            g.drawLine(xLat, xLon, yLat, yLon);
            g.setColor(Color.red);
        }
    }

    // 转换经纬度到地图的坐标，position指lat或者lon，maxLength是 width或者height
    private int transPosition(double position, double maxPosition, double minPosition, int maxLength){
        return (int) ((maxLength * 0.8 * (position - minPosition) / (maxPosition - minPosition)) + maxLength*0.1);
    }

}


