package customfilters;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

import java.io.IOException;

public class EnHunspellFilter extends TokenFilter {

    private final EnglishAnalyzer englishAnalyzer = new EnglishAnalyzer();

    public EnHunspellFilter(TokenStream input) {
        super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
        while (input.incrementToken()) {


            return true;
        }
        return false;
    }

}