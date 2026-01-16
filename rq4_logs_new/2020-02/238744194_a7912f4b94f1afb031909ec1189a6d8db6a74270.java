package IO;

public class Options {
    private boolean isUsingHillClimb;
    private String fileToRead;

    public Options(String fileToRead, boolean isUsingHillClimb) {
        this.isUsingHillClimb = isUsingHillClimb;
        this.fileToRead = fileToRead;
    }

    public boolean isUsingHillClimb() {
        return this.isUsingHillClimb;
    }

    public boolean isUsingGeneticAlgorithm() {
        return !this.isUsingHillClimb;
    }

    public String getFileToRead() {
        return fileToRead;
    }
}