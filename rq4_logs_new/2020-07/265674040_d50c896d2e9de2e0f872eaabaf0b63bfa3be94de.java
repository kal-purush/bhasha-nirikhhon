package org.monarchinitiative.vmvt.cli.commands;

import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AbstractSequenceComparisonCommand {

    @CommandLine.Option(names = {"-o","--out"})
    protected String outname = "ruler.svg";
    @CommandLine.Option(names = {"-r", "--ref"})
    protected String reference;
    @CommandLine.Option(names = {"-a", "--alt"})
    protected String alternate;
    @CommandLine.Option(names = {"-i", "--in"})
    protected String infile;
    /** Length of the sequences. Must be either 9 (donor) or 27 (acceptor) */
    protected int seqlen;

    /**
     * Users are allowed t pass both sequences on the command line or to pass the path to
     * a file that contains ref on the first line and alt on the second line. This
     * function checks if the user has p[assed the -i/--in flag and if so it reads the
     * sequences from the file. If not, it checks that the user passed both the
     * -r/--ref and -a/--alt flags. If there is an error, the function exits with
     * an error message.
     */
    protected void initSequences() {
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
        int reflen = reference.length();
        int altlen = alternate.length();
        if (reflen != altlen) {
            System.err.printf("[ERROR] Ref (%d)/Alt(%d) have different lengths. Both must be 9 nt for" +
                    " donor and 27 nt for acceptor sequences.\n", reflen, altlen);
            System.exit(1);
        }
        this.seqlen = reflen;
        if (seqlen != 9 && seqlen != 27) {
            System.err.printf("[ERROR] sequence length must be 9 nt for" +
                    " donor and 27 nt for acceptor sequences, but we got %d.\n", seqlen);
            System.exit(1);
        }
    }


    protected void readInputFile() {
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
}