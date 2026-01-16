package software.sebastian.oposiciones.service;

import java.util.Optional;
import java.time.LocalDateTime;

import org.springframework.stereotype.Service;

import com.openai.client.OpenAIClient;

import software.sebastian.oposiciones.model.Convocatoria;
import software.sebastian.oposiciones.repository.ConvocatoriaRepository;
import software.sebastian.oposiciones.repository.ModeloEmbeddingRepository;
import software.sebastian.oposiciones.repository.ModeloRepository;

@Service
public class ConvocatoriaEmbeddingService extends ModeloEmbeddingService<Convocatoria> {
  private final ConvocatoriaRepository convocatoriaRepo;

  public ConvocatoriaEmbeddingService(OpenAIClient openAIClient,
                                      ModeloRepository modeloRepo,
                                      ModeloEmbeddingRepository modeloEmbeddingRepo,
                                      ConvocatoriaRepository convocatoriaRepo) {
    super(openAIClient, modeloRepo, modeloEmbeddingRepo);
    this.convocatoriaRepo = convocatoriaRepo;
  }

  @Override
  public Convocatoria loadEntity(Integer id) {
    return convocatoriaRepo.findById(id)
        .orElseThrow(() -> new IllegalArgumentException("Convocatoria no encontrada: " + id));
  }

  @Override
  public Integer getEntityId(Convocatoria c) {
    return c.getConvocatoriaId();
  }

  @Override
  public LocalDateTime getEntityUpdatedAt(Convocatoria c) {
    return c.getUpdatedAt();
  }

  @Override
  public String buildInput(Convocatoria c) {
    String titulo = Optional.ofNullable(c.getTitulo()).orElse("");
    String texto  = Optional.ofNullable(c.getTexto()).orElse("");
    return (titulo + "\n\n" + texto).trim();
  }

  public String entityType = "CONVOCATORIA";
}