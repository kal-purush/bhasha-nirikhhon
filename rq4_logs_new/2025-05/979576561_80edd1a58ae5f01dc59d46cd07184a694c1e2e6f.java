package software.sebastian.oposiciones;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import software.sebastian.oposiciones.service.EtiquetaEmbeddingBatchService;

public class EmbeddingOneTimeRunner {

    public static void main(String[] args) {
        // 1) Arranca el contexto de Spring Boot
        ConfigurableApplicationContext ctx =
            SpringApplication.run(OposicionesApplication.class, args);

        // 2) Obtiene el servicio que procesa todas las etiquetas
        EtiquetaEmbeddingBatchService batchService =
            ctx.getBean(EtiquetaEmbeddingBatchService.class);

        // 3) Lanza la generación de embeddings una única vez
        batchService.generarEmbeddingsParaTodasLasEtiquetas();
        System.out.println("→ Embeddings generados para todas las etiquetas.");

        // 4) Termina la aplicación
        System.exit(0);
    }
}