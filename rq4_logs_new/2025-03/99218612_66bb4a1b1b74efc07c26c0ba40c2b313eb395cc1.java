package cli;

import workflows.negativeWGMs.NegativeWGM_ObservedLRT;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "negObs", description = "Observed LRT on negative WGMs", mixinStandardHelpOptions = true)
public class Neg_ObsLR_cmd implements Runnable{
    @Option(names = {"-t", "--tree"}, description = "Species tree filepath", paramLabel = "SPECIES_TREE", required = true)
    private String treeFile;

    @Option(names = {"-w", "--wgm"}, description = "WGM list filepath", paramLabel = "WGM_LIST", required = true)
    private String wgdFile;

    @Option(names = {"-gf", "--gene_family_rank"}, description = "Gene family rank (starts from 0)", paramLabel = "CURRENT_GF_RANK", required = true)
    private int gfNumber;

    @Option(names = {"-r", "--ranking"}, description = "Gene family ranking filepath", paramLabel = "LAMBDA_RANKING", required = true)
    private String combinedOutputFile;

    @Option(names = {"-c", "--count_profile"}, description = "Gene count profile filepath", paramLabel = "COUNT_PROFILE", required = true)
    private String gfCountsFile;

    @Option(names = {"-out", "--output_file"}, description = "Output filepath", paramLabel = "OUTPUT_FILE", required = true)
    private String outputFile;


    @Override
    public void run(){

        try (PrintStream output = new PrintStream(new FileOutputStream(outputFile))) {
            // Redirect stdout to the output file
            System.setOut(output);
            
            // Create and execute the NegativeWGM_ObservedLRT instance
            NegativeWGM_ObservedLRT lrtDist = new NegativeWGM_ObservedLRT(treeFile, wgdFile, gfNumber, combinedOutputFile, gfCountsFile);
            lrtDist.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Restore stdout
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        }
    }
}