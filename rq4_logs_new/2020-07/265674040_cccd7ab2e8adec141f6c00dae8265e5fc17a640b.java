package org.monarchinitiative.vmvt.cli.commands;


import org.monarchinitiative.vmvt.core.VmvtGenerator;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Create an SVG representing the distribution of ESE scores
 */
@CommandLine.Command(name = "ese", aliases = {"E"}, description = "Create ESE svg")
public class EseCommand extends AbstractSequenceComparisonCommand implements Callable<Integer> {
    /** Length, either 6 (hexamer) or 7 (heptameter) */
    @CommandLine.Option(names = {"-l", "--len"}, defaultValue = "6")
    protected int eseLength;


    @Override
    public Integer call() throws Exception {
        VmvtGenerator vmvt = new VmvtGenerator();
        String svg="";
        if (this.infile != null && this.reference==null && this.alternate==null) {
            readInputFile();
        }
        if (this.reference == null) {
            System.err.println("[ERROR] -r/--ref cannot be null (it should have the reference sequence");
            System.exit(1);
        } else if (this.alternate == null) {
            System.err.println("[ERROR] -r/--ref cannot be null (it should have the reference sequence");
            System.exit(1);
        }
        if (eseLength == 6) {
            svg = vmvt.getHexamerSvg(reference, alternate);
        } else if (eseLength == 7) {
            svg = vmvt.getHeptamerSvg(reference, alternate);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outname))) {
            writer.write(svg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}