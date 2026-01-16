import java.util.List;
import java.io.File;

public final class CrimeSearcher {
    public static void main(String args[]) {
        // TODO this class will look very different later on
        // the point right now is just to give fake data to the database
        File file = new File("data.csv");
        List<Crime> crimes = CSVParser.parseFile(file, "Atlanta");
        CrimeUploader.uploadCrime(crimes);
    }

    // Class contains static methods only and should
    // not be instantiated
    private CrimeSearcher() {}
}