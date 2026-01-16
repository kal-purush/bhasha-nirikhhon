package com.web.crawler.replacer;

import com.web.crawler.replacer.replacement.Replacement;
import com.web.crawler.model.Page;
import com.web.crawler.replacer.replacement.primitive.AddExtension;
import com.web.crawler.replacer.replacement.primitive.DontAddExtension;

import java.util.ArrayList;

public class ReplacerProcessor implements Replacer {

    private ArrayList<Replacement> replacements;

    public ReplacerProcessor () {
        replacements = new ArrayList<>();
        replacements.add(new AddExtension());
        replacements.add(new DontAddExtension());
    }

    @Override
    public String makeLocal(Page page, String link) {

        Replacement replacement = getOperation(link);
        if (replacement != null) {
            return replacement.replace(link);
        }
        return link;
    }

    private Replacement getOperation(String link) {

        for (Replacement replacement : replacements) {
            if (replacement.supports(link)) {
                return replacement;
            }
        }
        return null;
    }

}