package cli;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "BirthDeathModel",
        description = "Welcome to BirthDeathModel CLI! Complementary log-likelihood ratio tests (cLRTs) for whole-genome multiplication (WGM) inference.",
        subcommands = {Pos_ObsLR_cmd.class, Pos_SimH0Full_cmd.class, Pos_SimH0Rm_cmd.class, Pos_PostProc_cmd.class,  Neg_ObsLR_cmd.class, Neg_SimH0Full_cmd.class, Neg_SimH0Neg_cmd.class, Neg_PostProc_cmd.class},
        mixinStandardHelpOptions = true,
        version = "2.0.0")
public class BirthDeathModel_CLI implements Runnable {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new BirthDeathModel_CLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        System.out.println("Welcome to BirthDeathModel CLI! Please run '-h' for usage.");
    }
}