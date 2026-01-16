package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.exporting;

import fr.lip6.move.pnml.framework.general.PnmlExport;
import fr.lip6.move.pnml.framework.utils.ModelRepository;
import fr.lip6.move.pnml.framework.utils.exception.*;
import fr.lip6.move.pnml.ptnet.hlapi.*;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.Arc;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.LabeledVertex;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.PetriNet;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IntoPnmlExporter {

    private PageHLAPI pageHLAPI;
    private Map<String, NodeHLAPI> nodes;

    public void export(@NonNull PetriNet<Place, Transition> petriNet, @NonNull String path) throws InvalidIDException,
            VoidRepositoryException, OtherException, IOException, BadFileFormatException,
            OCLValidationFailed, UnhandledNetType, ValidationFailedException {

        ModelRepository.getInstance().createDocumentWorkspace("void");

        PetriNetDocHLAPI petriNetDocHLAPI = new PetriNetDocHLAPI();

        PetriNetHLAPI petriNetHLAPI = new PetriNetHLAPI("net", PNTypeHLAPI.PTNET);

        petriNetDocHLAPI.addNetsHLAPI(petriNetHLAPI);

        pageHLAPI = new PageHLAPI("page");

        petriNetHLAPI.addPagesHLAPI(pageHLAPI);

        nodes = new HashMap<>();

        addPlacesToPage(petriNet.getPlaces());
        addTransitionsToPage(petriNet.getTransitions());

        addArcs(petriNet.getPlaces());

        ModelRepository.getInstance().setPrettyPrintStatus(true);

        PnmlExport pex = new PnmlExport();
        pex.disableOclChecking();
        pex.exportObject(petriNetDocHLAPI, path);

        ModelRepository.getInstance().destroyCurrentWorkspace();
    }

    private void addPlacesToPage(@NotNull Collection<? extends Place> places)
            throws InvalidIDException, VoidRepositoryException {

        for (Place p : places) {
            PlaceHLAPI placeHLAPI = new PlaceHLAPI(p.label());

            if (p.getMarks() != 0) {
                placeHLAPI.setInitialMarkingHLAPI(new PTMarkingHLAPI(p.getMarks()));
            }

            pageHLAPI.addObjectsHLAPI(placeHLAPI);

            nodes.put(placeHLAPI.getId(), placeHLAPI);
        }
    }

    private void addTransitionsToPage(@NotNull Collection<? extends Transition> transitions)
            throws InvalidIDException, VoidRepositoryException {

        for (Transition t : transitions) {
            TransitionHLAPI transitionHLAPI = new TransitionHLAPI(t.label());
            pageHLAPI.addObjectsHLAPI(transitionHLAPI);

            nodes.put(transitionHLAPI.getId(), transitionHLAPI);
        }
    }

    private void addArcs(@NotNull Collection<? extends Place> places)
            throws InvalidIDException, VoidRepositoryException {

        int counter = 0;
        for (Place p : places) {
            for (Arc<Long, Long, Place, Transition> arc : p.getOutputs()) {
                addArcToPage(arc, counter++);
            }

            for (Arc<Long, Long, Transition, Place> arc : p.getInputs()) {
                addArcToPage(arc, counter++);
            }
        }
    }

    private <T1 extends LabeledVertex, T2 extends LabeledVertex>
    void addArcToPage(@NotNull Arc<Long, Long, T1, T2> arc, int counter)
            throws InvalidIDException, VoidRepositoryException {

        ArcHLAPI arcHLAPI = new ArcHLAPI("arc" + counter,
                nodes.get(arc.getFromEndpoint().label()),
                nodes.get(arc.getToEndpoint().label()));

        if (arc.weight() != 1) {
            arcHLAPI.setInscriptionHLAPI(new PTArcAnnotationHLAPI(arc.weight()));
        }

        pageHLAPI.addObjectsHLAPI(arcHLAPI);
    }

}