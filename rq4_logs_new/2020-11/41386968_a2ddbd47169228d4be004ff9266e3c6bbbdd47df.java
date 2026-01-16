// Copyright 2020 The Terasology Foundation
// SPDX-License-Identifier: Apache-2.0

package org.terasology.workstationInGameHelp.ui;

import com.google.common.collect.Lists;
import org.terasology.inGameHelpAPI.ui.WidgetFlowRenderable;
import org.terasology.rendering.assets.texture.TextureRegion;
import org.terasology.rendering.nui.widgets.browser.data.ParagraphData;
import org.terasology.rendering.nui.widgets.browser.data.basic.FlowParagraphData;
import org.terasology.rendering.nui.widgets.browser.data.basic.flow.FlowRenderable;
import org.terasology.rendering.nui.widgets.browser.data.basic.flow.ImageFlowRenderable;
import org.terasology.rendering.nui.widgets.browser.data.basic.flow.TextFlowRenderable;
import org.terasology.utilities.Assets;
import org.terasology.workstation.process.DescribeProcess;
import org.terasology.workstation.process.ProcessPartDescription;
import org.terasology.workstation.process.WorkstationProcess;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class WorkstationProcesses {
    private WorkstationProcesses() {
    }

    /**
     * Get the paragraph data of crafting recipes for this {@link WorkstationProcess}, printing the outputs first.
     *
     * <pre>
     *     output ["+" output]* "=" input ["+" input]*
     * </pre>
     *
     * @param process the process to display the output-focused recipe for
     * @return a paragraph containing the process description for the given workstation process
     */
    public static ParagraphData getInputHelpParagraphs(WorkstationProcess process) {
        return getParagraphData(process, true);
    }

    /**
     * Get the paragraph data of crafting recipes for this {@link WorkstationProcess}, printing the outputs last.
     *
     * <pre>
     *     input ["+" input]* "=" output ["+" output]*
     * </pre>
     *
     * @param process the process to display the input-focused recipe for
     * @return a paragraph containing the process description for the given workstation process
     */
    public static ParagraphData getOutputHelpParagraphs(WorkstationProcess process) {
        return getParagraphData(process, false);
    }

    /**
     * Gets the description of the workstation process.
     *
     * @param workstationProcess the workstation process to get the description from.
     * @return a list of paragraph data for workstationProcess.
     */
    protected static ParagraphData getParagraphData(WorkstationProcess workstationProcess, boolean outputFirst) {
        FlowParagraphData paragraphData = new FlowParagraphData(null);

        if (workstationProcess instanceof DescribeProcess) {
            ImageFlowRenderable plus = renderableFromTexture("workstation:plus");
            ImageFlowRenderable eq = renderableFromTexture("workstation:equals");

            DescribeProcess describeProcess = (DescribeProcess) workstationProcess;

            List<FlowRenderable> inputs = describeProcess.getInputDescriptions().stream()
                    .map(WorkstationProcesses::renderableFromDescription)
                    .collect(joining(plus));

            List<FlowRenderable> outputs = describeProcess.getOutputDescriptions().stream()
                    .map(WorkstationProcesses::renderableFromDescription)
                    .collect(joining(plus));

            if (outputFirst) {
                paragraphData.append(outputs);
                paragraphData.append(eq);
                paragraphData.append(inputs);
            } else {
                paragraphData.append(inputs);
                paragraphData.append(eq);
                paragraphData.append(outputs);
            }

        } else {
            paragraphData.append(new TextFlowRenderable(workstationProcess.getId() + " cannot be displayed", null,
                    null));
        }

        return paragraphData;
    }

    /**
     * Collect a stream to a list by joining the elements with the given delimiter.
     * <p>
     * The delimiter is only added between elements, not at the start or end of the stream.
     * </p>
     * <p>
     * Given a stream {@code buttons} of UI widgets {@code button1} ... {@code buttonN} joining this stream with another
     * UI widget {@code spacer} yields a list of spacer-separated buttons:
     * <pre>
     *         button1, spacer, button2, spacer, ..., spacer, buttonN
     *     </pre>
     * </p>
     *
     * @param delimiter the delimiter to interleave between elements of the stream.
     * @param <T> the type of elements in the stream
     * @param <U> the type of the delimiter
     * @return A {@code Collector} which concatenates stream elements,separated by the specified delimiter, in encounter
     *         order
     * @see Collectors#joining(CharSequence)
     */
    private static <T, U extends T> Collector<T, ?, ArrayList<T>> joining(U delimiter) {
        return Collectors.reducing(new ArrayList<>(), Lists::newArrayList, (acc, o) -> {
            if (!acc.isEmpty()) {
                acc.add(delimiter);
            }
            acc.addAll(o);
            return acc;
        });
    }

    //TODO: move this somewhere more accessible, e.g., to WidgetFlowRenderable or FlowRenderables?
    private static WidgetFlowRenderable renderableFromDescription(ProcessPartDescription description) {
        String hyperlink =
                description.getResourceUrn() != null ? description.getResourceUrn().toString() : null;
        return new WidgetFlowRenderable(description.getWidget(), 48, 48, hyperlink);
    }

    //TODO: move this somewhere more accessible, e.g., to ImageFlowRenderable or FlowRenderables?
    private static ImageFlowRenderable renderableFromTexture(String simpleUri) {
        TextureRegion plusTexture = Assets.getTextureRegion(simpleUri).get();
        return new ImageFlowRenderable(plusTexture, plusTexture.getWidth(),
                plusTexture.getWidth(), null);
    }
}