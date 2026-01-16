package com.controller.Dao;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.controller.Dao.implement.AdapterDao;
import com.controller.tda.graph.GraphLabelNoDirect;
import com.controller.tda.graph.algoritmos.BusquedaAnchura;
import com.controller.tda.graph.algoritmos.BellmanFord;
import com.controller.tda.graph.algoritmos.Floyd;
import com.controller.tda.list.LinkedList;
import com.models.CargaElectrica;

public class CargaElectricaDao extends AdapterDao<CargaElectrica> {

    private CargaElectrica cargaElectrica;
    private LinkedList<CargaElectrica> listAll;
    private GraphLabelNoDirect<String> graph;
    private LinkedList<String> vertexName;
    private String name = "CargaElectricagrafo.json";

    public GraphLabelNoDirect<String> creategraph() {
        if (vertexName == null) {
            vertexName = new LinkedList<>();
        }
        LinkedList<CargaElectrica> list = this.getListAll();
        if (!list.isEmpty()) {
            if (graph == null) {
                System.out.println("Grafo " + graph);
                graph = new GraphLabelNoDirect<>(list.getSize(), String.class);
            }
            
            CargaElectrica[] cargasElectricas = list.toArray();
            for (int i = 0; i < cargasElectricas.length; i++) {
                this.graph.labelsVertices(i + 1, cargasElectricas[i].getCarga());
                System.out.println("Vertices " + vertexName);

                vertexName.add(cargasElectricas[i].getCarga());
            }
            this.graph.drawGraph();
        }
        System.out.println("Grafo creado " + graph);
        return this.graph;
    }

    // Guardar el grafo en un archivo
    public void saveGraph() throws Exception {
        this.graph.saveGraphLabel(name);
    }
    
   
    public JsonArray obtainWeights() throws Exception {
        if (graph == null) {
            creategraph();
        }
    
        if (graph.existsFile(name)) {
            graph.cargarModelosDesdeDao();
            graph.loadGraph(name);
    
            JsonArray graphData = graph.obtainWeights();
            System.out.println("Modelo de vis,js " + graphData);
            return graphData; 
        } else {
            throw new Exception("El archivo de grafo no existe.");
        }
    }
    
    public JsonObject getGraphData() throws Exception {
        if (graph == null) {
            creategraph();
        }
    
        if (graph.existsFile(name)) {
            graph.cargarModelosDesdeDao();
            graph.loadGraph(name);
    
            JsonObject graphData = graph.getVisGraphData(); 
            System.out.println("Modelo de vis,js " + graphData);
            return graphData; 
        } else {
            throw new Exception("El archivo de grafo no existe.");
        }
    }
    
    public GraphLabelNoDirect<String> crearuniosnes() throws Exception {
        if (graph == null) {
            creategraph();
        }
        if (graph.existsFile(name)) {
            graph.cargarModelosDesdeDao();
            graph.loadGraphWithRandomEdges(name);
            System.out.println("Modelo asociado al grafo: " + name);
        } else {
            throw new Exception("El archivo de grafo no existe.");
        }
        saveGraph();
        return graph;
    }

    public String busquedaanchura(int inicio) throws Exception {
        if (graph == null) {
            throw new Exception("El grafo no existe");
        }

        BusquedaAnchura busquedaanchuraAlgoritmo = new BusquedaAnchura(graph, inicio);

        String recorrido = busquedaanchuraAlgoritmo.recorrerGrafo();
        return recorrido;
    }

    public String caminoCorto(int inicio, int fin, int algoritmo) throws Exception {
        if (graph == null) {
            throw new Exception("Grafo no existe");
        }

        System.out.println("Calculando camino corto desde " + inicio + " hasta " + fin);

        String camino = "";

        if (algoritmo == 1) { 
            Floyd floydWarshall = new Floyd(graph, inicio, fin);
            camino = floydWarshall.caminoCorto(); 
        } else { 
            BellmanFord bellmanFord = new BellmanFord(graph, inicio, fin);
            camino = bellmanFord.caminoCorto(); 
        }

        System.out.println("Camino corto calculado: " + camino);	
        return camino;
    }

    public boolean existeGrafo() {
        if (graph == null) {
            try {
                graph = new GraphLabelNoDirect<>(10, String.class);
            } catch (Exception e) {
                return false;
            }
        }
        return graph.existsFile(name);
    }

    public String calcularRutaEnergia(Integer inicio, Integer fin) throws Exception {
        try {
            if (graph == null) {
                graph = new GraphLabelNoDirect<>(10, String.class);
            }
            
            if (!existeGrafo()) {
                throw new Exception("El grafo no existe. Por favor, cree el grafo primero desde la sección 'Mapa de Grafos'.");
            }

            // Cargar el grafo desde el archivo existente
            graph.cargarModelosDesdeDao();
            graph.loadGraph(name);
            
            // Verificar si hay conexiones, si no hay, crearlas
            boolean tieneConexiones = false;
            for (int i = 1; i <= graph.nro_vertices(); i++) {
                if (!graph.adyecencias(i).isEmpty()) {
                    tieneConexiones = true;
                    break;
                }
            }
            
            if (!tieneConexiones) {
                graph.loadGraphWithRandomEdges(name);
            }
            
            return graph.calcularRutaEnergia(inicio, fin);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error al calcular la ruta: " + e.getMessage());
        }
    }

    public CargaElectricaDao() {
        super(CargaElectrica.class);
    }

    public CargaElectrica getCargaElectrica() {
        if (cargaElectrica == null) {
            cargaElectrica = new CargaElectrica();
        }
        return this.cargaElectrica;
    }

    public void setCargaElectrica(CargaElectrica cargaElectrica) {
        this.cargaElectrica = cargaElectrica;
    }

    public LinkedList<CargaElectrica> getListAll() {
        if (this.listAll == null) {
            this.listAll = listAll();
        }
        return this.listAll;
    }

    public Boolean save() throws Exception {
        Integer id = getListAll().getSize() + 1;
        getCargaElectrica().setid(id);
        persist(getCargaElectrica());
        return true;
    }

    public Boolean update() throws Exception {
        this.merge(getCargaElectrica(), getCargaElectrica().getid() - 1);
        this.listAll = listAll();
        return true;
    }

    public Boolean delete(Integer id) throws Exception {
        for (int i = 0; i < getListAll().getSize(); i++) {
            CargaElectrica cargaElectrica = getListAll().get(i);
            if (cargaElectrica.getid().equals(id)) {
                getListAll().delete(i);
                return true;
            }
        }
        throw new Exception("CargaElectrica no encontrada con ID: " + id);
    }
} 