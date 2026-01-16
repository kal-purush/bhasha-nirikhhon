package application.services.impl;

import application.services.BuildBiologicalObjectService;
import domain.models.BiologicalObject;
import domain.models.TranscriptionFactor;
import domain.port.out.external_repositories.HGNCRepository;
import domain.port.out.repositories.BiologicalObjectRepository;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@ApplicationScoped
public class BiologicalObjectServiceImpl implements BuildBiologicalObjectService {

    private final HGNCRepository hgncRepository;
    private final BiologicalObjectRepository biologicalObjectRepository;

    @Override
    public BiologicalObject execute(String geneSymbol) {
        var biologicalObject = build(geneSymbol);
        return biologicalObjectRepository.save(biologicalObject);
    }

    @Override
    public BiologicalObject execute(TranscriptionFactor transcriptionFactor) {
        var biologicalObject = build(transcriptionFactor.name());
        biologicalObject.setTranscriptionFactor(transcriptionFactor);
        return biologicalObjectRepository.save(biologicalObject);
    }

    private BiologicalObject build(String geneSymbol) {
        var biologicalObject = new BiologicalObject();
        var hgncResponse = hgncRepository.findHGNCInformation(geneSymbol);

        if (hgncResponse != null) {
            biologicalObject.setName(hgncResponse.getName());
        }
        return biologicalObject;
    }

}