package com.akaene.stpa.scs.parser;

import com.akaene.stpa.scs.model.Model;
import com.akaene.stpa.scs.parser.graphml.GraphMLParser;
import com.akaene.stpa.scs.parser.sysml.SysMLXMIParser;

import java.io.File;
import java.util.List;

/**
 * Provides parsing of control structure for specified file.
 * <p>
 * It contains a list of supported parsers. When a file is provided, it finds the first parser that supports it and uses
 * it to parse the system control structure.
 */
public class ControlStructureParsers {

    private static final List<ControlStructureParser> parsers = List.of(
            new SysMLXMIParser(),
            new GraphMLParser()
    );

    /**
     * Finds a suitable parser and uses it to parse system control structure from the specified file.
     *
     * @param input File containing system model
     * @return Model of system control structure read from the specified file
     */
    public static Model parse(File input) {
        return parsers.stream().filter(p -> p.supports(input)).findFirst().map(p -> p.parse(input))
                      .orElseThrow(
                              () -> new IllegalArgumentException("No parser found that would support file " + input));
    }
}