package software.sebastian.oposiciones.service;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.theokanning.openai.service.OpenAiService;
import com.theokanning.openai.embeddings.EmbeddingCreateParams;
import software.sebastian.oposiciones.service.BoeEmbeddingService;

import software.sebastian.oposiciones.model.Convocatoria;
import software.sebastian.oposiciones.model.ModeloEmbedding;
import software.sebastian.oposiciones.repository.ConvocatoriaRepository;
import software.sebastian.oposiciones.repository.ModeloEmbeddingRepository;
import software.sebastian.oposiciones.repository.ModeloRepository;
import software.sebastian.oposiciones.service.TokenizerPythonCaller; // Ajusta el paquete si está en util

@Service
public class BoeEmbeddingService {
    private static final String EMBEDDING_MODEL = "text-embedding-3-large";
    private static final int MAX_TOKENS = 5000;

    private final OpenAiService openAIClient;
    private final ModeloRepository modeloRepo;
    private final ModeloEmbeddingRepository modeloEmbeddingRepo;
    private final ConvocatoriaRepository convocatoriaRepo;
    private final TokenizerPythonCaller tokenizer;

    public BoeEmbeddingService(OpenAiService openAIClient,
                               ModeloRepository modeloRepo,
                               ModeloEmbeddingRepository modeloEmbeddingRepo,
                               ConvocatoriaRepository convocatoriaRepo,
                               TokenizerPythonCaller tokenizer) {
        this.openAIClient = openAIClient;
        this.modeloRepo = modeloRepo;
        this.modeloEmbeddingRepo = modeloEmbeddingRepo;
        this.convocatoriaRepo = convocatoriaRepo;
        this.tokenizer = tokenizer;
    }

    @Transactional
    public void generarYGuardarEmbeddingBoe(Integer boeId) throws Exception {
        // 1) Cargar BOE
        Convocatoria boe = convocatoriaRepo.findById(boeId)
            .orElseThrow(() -> new IllegalArgumentException("BOE no encontrado: " + boeId));

        // 2) Concatenar y limpiar texto
        String texto = ((boe.getTitulo() != null ? boe.getTitulo().trim() : "")
                         + " "
                         + (boe.getTexto()  != null ? boe.getTexto().trim()  : ""))
                       .trim();
        if (texto.isEmpty()) {
            return;
        }

        // 3) Truncar según límite de tokens
        String truncated = tokenizer.truncarTextoPorTokens(
            EMBEDDING_MODEL, MAX_TOKENS, texto);

        // 4) Llamada a OpenAI para generar embedding
        List<Double> embList = openAIClient.embeddings().create(
            EmbeddingCreateParams.builder()
                .model(EMBEDDING_MODEL)
                .input(truncated)
                .build()
        ).data().get(0).embedding();

        // 5) Convertir List<Double> a double[]
        double[] embArray = embList.stream()
                                   .mapToDouble(Double::doubleValue)
                                   .toArray();

        // 6) Obtener modeloId
        Integer modeloId = modeloRepo.findByNombre(EMBEDDING_MODEL)\+           .orElseThrow(() -> new IllegalStateException("Modelo embedding no encontrado"))
            .getModeloId();

        // 7) Persistir embedding en BD
        ModeloEmbedding me = new ModeloEmbedding(modeloId, boeId, embArray);
        modeloEmbeddingRepo.save(me);
    }
}