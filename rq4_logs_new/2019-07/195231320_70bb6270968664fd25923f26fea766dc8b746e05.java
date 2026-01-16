package Generator;

import Model.ConstCollection;
import Model.ControlTypes;

import java.util.ArrayList;

public class GeneratorRetranslator {

    public void startGenerator(ArrayList<ControlTypes> list, int amount, boolean contrast, boolean disabledControls, boolean isSorted) {
        Generator gen = new Generator();
        if (list == null) {
            gen.createSamples(null, amount, contrast, disabledControls, isSorted);
        } else {
            list.forEach((control) -> gen.createSamples(control, amount, contrast, disabledControls, isSorted));
        }
    }
}