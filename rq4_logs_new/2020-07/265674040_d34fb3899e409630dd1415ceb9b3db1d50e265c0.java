package org.monarchinitiative.vmvt.cli.commands;


import org.monarchinitiative.vmvt.core.VmvtGenerator;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "delta", aliases = {"D"}, mixinStandardHelpOptions = true, description = "Create Delta svg")
public class DeltaCommand extends AbstractSequenceComparisonCommand implements Callable<Integer> {



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
        // same function for acceptor and donor, the Delta class figures out the length.
        svg = vmvt.getDelta(reference, alternate);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outname))) {
            writer.write(svg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}