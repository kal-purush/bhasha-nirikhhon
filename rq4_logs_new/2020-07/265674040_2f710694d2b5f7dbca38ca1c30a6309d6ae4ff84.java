package org.monarchinitiative.vmvt.cli.commands;


import org.monarchinitiative.vmvt.core.VmvtGenerator;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "trek", aliases = {"T"}, description = "Create sequence trekker")
public class TrekCommand extends AbstractSequenceComparisonCommand implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        VmvtGenerator vmvt = new VmvtGenerator();
        String svg="";
        initSequences();
        if (seqlen == 9) {
            // donor
            svg= vmvt.getDonorTrekkerSvg(this.reference, this.alternate);
        } else {
            svg = vmvt.getAcceptorTrekkerSvg(this.reference, this.alternate);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outname))) {
            writer.write(svg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}