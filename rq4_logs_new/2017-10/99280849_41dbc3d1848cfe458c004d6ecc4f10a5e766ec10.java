package tk.hausplant.view;

import spacedrawboard.resource.Material;
import spacedrawboard.resource.Mesh;
import spacedrawboard.resource.Model;
import spacedrawboard.resource.Triangle;
import spacedrawboard.resource.Vector3D;
import spacedrawboard.visualization.Drawboard;
import spacedrawboard.visualization.Visualization;
import tk.hausplant.model.Parede;
import tk.hausplant.model.Planta;

public class Renderizador3DPlanta {

    private final Planta planta;

    public final Drawboard drawboard = new Drawboard();

    public Renderizador3DPlanta(Planta planta) {
        this.planta = planta;
    }

    private void construirParedes() {
        Material materialParede = new Material();

        // Montar paredes 3D usando triângulos
        for (Parede parede : planta.getWalls()) {
            // Cantos da parede
            Vector3D A = new Vector3D(parede.getA().x, parede.getA().y, 0);
            Vector3D Ah = new Vector3D(parede.getA().x, parede.getA().y, 0);
            
            Vector3D B = new Vector3D(parede.getB().x, parede.getB().y, 0);
            Vector3D Bh = new Vector3D(parede.getB().x, parede.getB().y, 0);
            
            // Ajustar escalas dos cantos (converter de pixel para metro)
            A = A.timesScalar(1/Prancheta.PIXELS_POR_METRO);
            Ah = Ah.timesScalar(1/Prancheta.PIXELS_POR_METRO);
            B = B.timesScalar(1/Prancheta.PIXELS_POR_METRO);
            Bh = Bh.timesScalar(1/Prancheta.PIXELS_POR_METRO);
            
            // Adicionar altura às paredes
            Ah.z = Parede.ALTURA_PADRAO;
            Bh.z = Parede.ALTURA_PADRAO;
            
            Mesh formaParede = new Mesh();
            formaParede.addTriangle(new Triangle(A, Ah, Bh));
            formaParede.addTriangle(new Triangle(Bh, B, A));

            Model modeloParede = new Model(formaParede, materialParede);
            drawboard.addModel(modeloParede);
        }
    }

    public void mostrarVisualizacao() {
        construirParedes();
        
        // Mostrar visualização da cena
        Visualization visualizacao = new Visualization("Planta", 900, 600, drawboard);
        visualizacao.setZoomSpeed(2);
        visualizacao.setDragSpeed(0.03);
        visualizacao.showWindow();
    }

}