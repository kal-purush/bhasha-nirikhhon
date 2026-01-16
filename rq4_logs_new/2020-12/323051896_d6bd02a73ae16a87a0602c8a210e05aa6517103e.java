import graph.Utils.DefaultVertexFactory;
import graph.Utils.VertexFactory;
import graph.algorithms.Dinits;
import graph.algorithms.EdmondsCarp;
import graph.algorithms.PushingDownstream;
import graph.common.Vertex;
import graph.common.flow.FlowEdge;
import graph.common.flow.FlowGraph;
import graph.impl.defaultImpl.DefaultVertex;
import graph.impl.flowImpl.DefaultFlowEdge;
import graph.impl.flowImpl.DefaultFlowGraph;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class Main {
    private static FlowGraph<Vertex, FlowEdge<Vertex>> graph;
    private static Vertex s;
    private static Vertex t;

    private static final int VERTEX_COUNT_IN_LAYER = 20;
    private static final int LAYER_COUNT = 10;


    public static void main(String[] args) {
        graph = new DefaultFlowGraph<>();
        VertexFactory<DefaultVertex> vertexFactory = new DefaultVertexFactory();

        //добавление истока и стока
        s = vertexFactory.createVertex("s");
        t = vertexFactory.createVertex("t");
        graph.addVertex(s);
        graph.addVertex(t);

        Set<Vertex> vertexesOld = new HashSet<>();
        Set<Vertex> vertexesNew;
        vertexesOld.add(s);
        //генерация слоев графа
        for(int layerNumber = 0; layerNumber < LAYER_COUNT; layerNumber++){
            vertexesNew = new HashSet<>();
            for(int vertexNumber = 0; vertexNumber < VERTEX_COUNT_IN_LAYER; vertexNumber++){
                Vertex vertex = vertexFactory.createVertex();
                vertexesNew.add(vertex);
                graph.addVertex(vertex);
            }
            //добавление ребер между вершинами
            for (Vertex vertexOld : vertexesOld){
                for (Vertex vertexNew : vertexesNew){
                    int randomCapacity = ThreadLocalRandom.current().nextInt(2, 100 + 1);
                    graph.addEdge(new DefaultFlowEdge<>(vertexOld, vertexNew, randomCapacity));
                }
            }
            vertexesOld = vertexesNew;
        }

        for (Vertex vertexOld : vertexesOld){
            int randomCapacity = ThreadLocalRandom.current().nextInt(2, 100 + 1);
            graph.addEdge(new DefaultFlowEdge<>(vertexOld, t, randomCapacity));
        }

        int expectedFlow = EdmondsCarp.findMaxFlow(s, t, graph);
        System.out.println("ExpectedFlow EdmondsCarp: " + expectedFlow);
        expectedFlow = Dinits.findMaxFlow(s, t, graph);
        System.out.println("ExpectedFlow Dinits: " + expectedFlow);
        expectedFlow = PushingDownstream.findMaxFlow(s, t, graph);
        System.out.println("ExpectedFlow PushingDownstream: " + expectedFlow);
    }




}