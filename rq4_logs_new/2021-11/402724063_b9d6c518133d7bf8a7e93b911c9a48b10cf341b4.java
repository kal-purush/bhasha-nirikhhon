package seedu.academydirectory.versioncontrol.reader;

import static java.util.Objects.requireNonNull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import seedu.academydirectory.versioncontrol.objects.Label;

public class LabelReader extends VersionControlObjectReader<Label> {
    private final CommitReader commitReader;

    /**
     * Creates a Reader which reads {@code Label} from disk
     * @param vcPath Path to where labels to be read are located
     * @param commitReader a CommitReader instance
     */
    public LabelReader(Path vcPath, CommitReader commitReader) {
        super(vcPath);
        requireNonNull(commitReader);
        this.commitReader = commitReader;
    }

    @Override
    protected Label getProgrammableFormat(List<String> responseArr) {
        assert responseArr.size() >= 1; // Will contain at least filename
        String name = responseArr.get(0);
        Iterator<String> args = responseArr.listIterator(1);

        List<String> fields = new ArrayList<>();
        while (args.hasNext()) {
            String[] arg = args.next().split(": ");
            if (arg.length == 2) {
                fields.add(arg[1]);
            }
        }

        if (fields.size() != 1) {
            return Label.emptyLabel();
        }

        return Label.of(name, name, () -> commitReader.read(fields.get(0)));
    }
}