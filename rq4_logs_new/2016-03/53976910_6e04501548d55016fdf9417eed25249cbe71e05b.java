package edu.uci.ics.textdb.dataflow.regexmatch;

import edu.uci.ics.textdb.api.common.IDocument;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

import java.util.List;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcher implements IOperator {

    private final List<IDocument> documentSet;
    private final String regex;
    private ISourceOperator sourceOperator;

    public RegexMatcher(List<IDocument> documentSet, String regex, ISourceOperator sourceOperator) {
        this.documentSet = documentSet;
        this.regex = regex;
        this.sourceOperator = sourceOperator;
    }

    public ITuple getNextTuple() {

        ITuple sourceTuple = sourceOperator.getNextTuple();
        if (match(sourceTuple)) {
            return sourceTuple;
        } else {
            return getNextTuple();
        }

    }

    private boolean match(ITuple sourceTuple) {
        return false;
    }
}