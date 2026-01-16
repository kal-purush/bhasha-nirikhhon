package calculate;


import javafx.application.Platform;
import javafx.concurrent.Task;

import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Vai on 5/9/17.
 */
public class CalculateTask extends Task<ArrayList<Edge>> implements Observer {

    private final String side;
    private ArrayList<Edge> edges;
    private KochFractal k;
    private int i;
    private int maxEdges;
    private KochManager manager;

    public CalculateTask(String side, KochManager manager)
    {
        this.manager = manager;
        this.side = side;
        edges = new ArrayList<>();
        k = new KochFractal();
        k.addObserver(this);
        k.setLevel(manager.getLevel());

        maxEdges = k.getNrOfEdges() / 3;
    }

    public KochFractal getK() {
        return k;
    }

    public void setK(KochFractal k) {
        this.k = k;
    }


    @Override
    protected ArrayList<Edge> call() throws Exception {
        edges.clear();
        if(side.equals("Left")){
            k.generateLeftEdge();
        }
        else if(side.equals("Bottom")){
            k.generateBottomEdge();
        }
        else if(side.equals("Right")){
            k.generateRightEdge();
        }


        manager.finished();

        return edges;
    }

    @Override
    protected void cancelled() {
        super.cancelled();
        k.cancel();
    }

    @Override
    public void update(Observable o, Object arg) {

        edges.add((Edge)arg);
        manager.addEdge((Edge)arg);

        updateMessage("Edges" + edges.size());
        Edge e = (Edge)arg;
        Platform.runLater(() -> {
            i++;
            updateProgress(i, maxEdges);
            updateMessage(i + "/" + maxEdges);
            manager.drawEdge(e);
        });

        try {
            if (side.matches("Left")) {
                Thread.sleep(1);
            }
            else if (side.matches("Right")) {
                Thread.sleep(2);
            }
            else {
                Thread.sleep(3);
            }
        }
        catch (Exception exc) {
            Thread.currentThread().interrupt();
        }
    }
}