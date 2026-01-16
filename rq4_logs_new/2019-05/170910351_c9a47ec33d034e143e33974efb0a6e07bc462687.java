package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.PetriNet;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.PetriNetReader;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.ReductionHistory;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.analysis.Reducer;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.analysis.coverability.CoverabilityAnalyser;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.exporting.PnmlColoredResult;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.importing.PnmlReader;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions.DeleteRedundantPlace;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions.PostAgglomerationOfTransitions;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions.PreAgglomerationOfTransition;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static hse.se.aaizmaylov.petrinetslibrary.petrinets.TestUtil.getPath;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IntegrationTests {
    private static final Logger LOGGER = Logger.getLogger(IntegrationTests.class);

    private static String readAllFromFileAndDelete(String path) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            File file = new File(path);

            if (!file.delete()) {
                LOGGER.error("Cannot delete file " + path);
            }
        }
    }

    @Test
    void checkFullAnalysis() throws PetriNetReader.PetriNetReadingException, IOException, SAXException,
            ParserConfigurationException, TransformerException {

        String path = getPath("integration/murata17d.pnml", getClass());

        PnmlReader reader = new PnmlReader();

        PetriNet<Place, Transition> petriNet = reader.read(path);

        Reducer reducer = new Reducer(petriNet);

        ReductionHistory history = reducer.reduce(Arrays.asList(
                new PostAgglomerationOfTransitions(),
                new PreAgglomerationOfTransition(),
                new DeleteRedundantPlace()),
                Collections.emptyList());


        PnmlColoredResult result = new PnmlColoredResult(path);
        CoverabilityAnalyser analyser = new CoverabilityAnalyser(petriNet);

        for (Place p : analyser.getUnboundedPlaces()) {
            for (String pId : history.getAssociated(p)) {
                result.markPlaceUnbounded(pId);
            }
        }

        for (Transition t : analyser.getDeadTransitions()) {
            for (String tId : history.getAssociated(t)) {
                result.markTransitionDead(tId);
            }
        }

        Path path1 = Paths.get(path);
        Path res = path1.resolveSibling(path1.getFileName() + "_res");
        result.saveToFile(res.toString());

        LOGGER.info(readAllFromFileAndDelete(res.toString()));

        assertEquals(0, analyser.getUnboundedPlaces().size());
        assertEquals(0, analyser.getDeadTransitions().size());
    }

    @Test
    void checkFullAnalysisWithUnboundedPlace() throws PetriNetReader.PetriNetReadingException, IOException,
            SAXException, ParserConfigurationException, TransformerException {

        String path = getPath("integration/murata17e.pnml", getClass());

        PnmlReader reader = new PnmlReader();

        PetriNet<Place, Transition> petriNet = reader.read(path);

        Reducer reducer = new Reducer(petriNet);

        ReductionHistory history = reducer.reduce(Arrays.asList(
                new PostAgglomerationOfTransitions(),
                new PreAgglomerationOfTransition(),
                new DeleteRedundantPlace()),
                Collections.emptyList());


        PnmlColoredResult result = new PnmlColoredResult(path);
        CoverabilityAnalyser analyser = new CoverabilityAnalyser(petriNet);

        for (Place p : analyser.getUnboundedPlaces()) {
            for (String pId : history.getAssociated(p)) {
                result.markPlaceUnbounded(pId);
            }
        }

        for (Transition t : analyser.getDeadTransitions()) {
            for (String tId : history.getAssociated(t)) {
                result.markTransitionDead(tId);
            }
        }

        Path path1 = Paths.get(path);
        Path res = path1.resolveSibling(path1.getFileName() + "_res");
        result.saveToFile(res.toString());

        LOGGER.info(readAllFromFileAndDelete(res.toString()));

        assertEquals(1, analyser.getUnboundedPlaces().size());
        assertEquals(0, analyser.getDeadTransitions().size());
    }
}
