package org.monarchinitiative.vmvt.cli.commands;

import org.monarchinitiative.vmvt.core.VmvtGenerator;
import picocli.CommandLine;

import java.io.*;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Print an SVG with the donor or acceptor ruler.
 * @author Peter N Robinson
 */
@CommandLine.Command(name = "ruler", aliases = {"R"}, description = "Create sequence ruler")
public class RulerCommand implements Callable<Integer> {
    @CommandLine.Option(names = {"-o","--out"})
    String outname = "ruler.svg";
    @CommandLine.Option(names = {"-r", "--ref"})
    String reference;
    @CommandLine.Option(names = {"-a", "--alt"})
    String alternate;
    @CommandLine.Option(names = {"-i", "--in"})
    String infile;



    private void readInputFile() {
        if (this.infile == null) {
            System.err.println("[ERROR] -i/--in passed with null pointer");
            System.exit(1);
        }
        File f = new File(this.infile);
        if (! f.exists()) {
            System.err.println("[ERROR] input file not found (" + this.infile +")");
            System.exit(1);
        }
        try {
            List<String> lines = java.nio.file.Files.readAllLines(new File(this.infile).toPath());
            if (lines.size() != 2) {
                System.err.println("[ERROR] input file must have exactly two lines with " +
                        "the reference and the alternate sequence");
                System.exit(1);
            }
            this.reference = lines.get(0).trim();
            this.alternate = lines.get(1).trim();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Integer call() throws Exception {
        VmvtGenerator vmvt = new VmvtGenerator();
        String svg="";
        if (this.infile != null && this.reference==null && this.alternate==null) {
            readInputFile();
        }
        if (this.reference == null) {
            System.err.println("[ERROR] -r/--ref cannot be null (it should have the reference sequence");
            return 1;
        } else if (this.alternate == null) {
            System.err.println("[ERROR] -r/--ref cannot be null (it should have the reference sequence");
            return 1;
        }
        int reflen = reference.length();
        int altlen = alternate.length();
        if (reflen != altlen) {
            System.err.printf("[ERROR] Ref (%d)/Alt(%d) have different lengths. Both must be 9 nt for" +
                    " donor and 27 nt for acceptor sequences.\n", reflen, altlen);
            return 1;
        }
        int seqlen = reflen;
        if (seqlen != 9 && seqlen != 27) {
            System.err.printf("[ERROR] sequence length must be 9 nt for" +
                    " donor and 27 nt for acceptor sequences, but we got %d.\n", seqlen);
            return 1;
        }
        if (seqlen == 9) {
            // donort
            svg= vmvt.getDonorSequenceRuler(this.reference, this.alternate);
        } else {
            svg = vmvt.getAcceptorSequenceRuler(this.reference, this.alternate);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outname))) {
            writer.write(svg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}