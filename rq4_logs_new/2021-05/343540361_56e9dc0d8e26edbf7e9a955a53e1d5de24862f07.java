package problem609;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FindDuplicateFileInSystem {

    public List<List<String>> findDuplicate(final String[] paths) {
        final Map<String, List<String>> contentToPaths = new HashMap<>();
        final StringBuilder pathBuilder = new StringBuilder(3001);
        final StringBuilder contentBuilder = new StringBuilder(2000);

        for (final String p : paths) {

            final char[] pc = p.toCharArray();
            pathBuilder.setLength(0);

            int i = 0;
            while (pc[i] != ' ')
                pathBuilder.append(pc[i++]);

            pathBuilder.append('/');
            int baseLen = ++i;

            while (i < pc.length) {
                pathBuilder.setLength(baseLen);

                while (pc[i] != '(')
                    pathBuilder.append(pc[i++]);

                i++;
                contentBuilder.setLength(0);

                while (pc[i] != ')')
                    contentBuilder.append(pc[i++]);

                i += 2;
                contentToPaths.computeIfAbsent(contentBuilder.toString(), k -> new ArrayList<>()).add(pathBuilder.toString());
            }

        }

        final List<List<String>> result = new ArrayList<>();
        for (var en : contentToPaths.entrySet())
            if (en.getValue().size() > 1)
                result.add(en.getValue());

        return result;
    }

}