package org.bonej.wrapperPlugins;

import net.imagej.ImageJ;
import net.imagej.Main;
import org.scijava.ItemVisibility;
import org.scijava.command.Command;
import org.scijava.plugin.Parameter;
import org.scijava.plugin.Plugin;
import org.scijava.widget.ChoiceWidget;

/**
 * @tode Make previewable
 * @author <a href="mailto:rdomander@rvc.ac.uk">Richard Domander</a>
 */
@Plugin(type = Command.class, menuPath = "Plugins>BoneJ>TriplePointAngles")
public class TriplePointAngles implements Command
{
    private static final int DEFAULT_NTH_POINT = 0;

    @Parameter(label = "Use:", style = ChoiceWidget.LIST_BOX_STYLE, choices = {"Opposite vertex", "nth edge pixel"})
    private String pointChoiceString;

    @Parameter(label = "Nth point", min = "0")
    private int nthPoint = DEFAULT_NTH_POINT;

    @Parameter(visibility = ItemVisibility.MESSAGE)
    private final String helpUrl = "http://bonej.org/triplepointangles";

    @Override
    public void run() {
    }

    public static void main(final String... args)
    {
        ImageJ ij = Main.launch(args);
        ij.command().run(TriplePointAngles.class, true);
    }
}