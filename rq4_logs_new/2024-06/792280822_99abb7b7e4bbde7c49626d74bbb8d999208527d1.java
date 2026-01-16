package com.akaene.stpa.scs.parser.graphml;

import com.akaene.stpa.scs.model.Connector;
import com.akaene.stpa.scs.model.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphMLParserTest {

    private final GraphMLParser sut = new GraphMLParser();

    @ParameterizedTest
    @MethodSource("sampleFileTypes")
    void supportsReturnsTrueForSupportedFiles(String file, boolean supports) throws Exception {
        final File input = new File(getClass().getClassLoader().getResource(file).toURI());
        assertEquals(supports, sut.supports(input));
    }

    static Stream<Arguments> sampleFileTypes() {
        return Stream.of(
                Arguments.of("simple-model/model.xmi", false),
                Arguments.of("simple-model.zip", false),
                Arguments.of("simple-model.graphml", true)
        );
    }

    @Test
    void parserHandlesSimpleGraphMLFile() throws Exception {
        final File input = new File(getClass().getClassLoader().getResource("simple-model.graphml").toURI());
        final Model result = sut.parse(input);
        assertNotNull(result);
        final Optional<Connector> controlAction = result.getConnectors().stream()
                                                        .filter(c -> c.getName().equals("change altitude")).findAny();
        assertTrue(controlAction.isPresent());
        assertTrue(controlAction.get().getStereotypes().stream().anyMatch(s -> s.name().equals("ControlAction")));
        assertEquals("FlightCrew", controlAction.get().getSource().type().name());
        assertEquals("Flight", controlAction.get().getTarget().type().name());
        final Optional<Connector> feedback = result.getConnectors().stream().filter(c -> c.getName().equals("altitude"))
                                                   .findAny();
        assertTrue(feedback.isPresent());
        assertTrue(feedback.get().getStereotypes().stream().anyMatch(s -> s.name().equals("Feedback")));
        assertEquals("Flight", feedback.get().getSource().type().name());
        assertEquals("FlightCrew", feedback.get().getTarget().type().name());
    }
}