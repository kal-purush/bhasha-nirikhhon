package io.v.vdl;

import com.intellij.openapi.editor.colors.TextAttributesKey;
import com.intellij.openapi.fileTypes.SyntaxHighlighter;
import com.intellij.openapi.options.colors.AttributesDescriptor;
import com.intellij.openapi.options.colors.ColorDescriptor;
import com.intellij.openapi.options.colors.ColorSettingsPage;
import com.intellij.util.containers.ContainerUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;
import java.util.Map;

public class VdlColorSettingsPage implements ColorSettingsPage {
    private final Map<String, TextAttributesKey> attributes = new ContainerUtil
            .ImmutableMapBuilder<String, TextAttributesKey>()
                    .put("Line comment", VdlSyntaxHighlightingColors.LINE_COMMENT)
                    .put("Keyword", VdlSyntaxHighlightingColors.KEYWORD)
                    .put("Identifier", VdlSyntaxHighlightingColors.IDENTIFIER).build();

    @Nullable
    @Override
    public Icon getIcon() {
        return VdlIcons.FILE;
    }

    @NotNull
    @Override
    public SyntaxHighlighter getHighlighter() {
        return new VdlSyntaxHighlighter();
    }

    @NotNull
    @Override
    public String getDemoText() {
        return "package demo\n" +
                "\n" +
                "\n" +
                "// This is a line comment.\n" +
                "type Foo() {\n" +
                "\tBar(int32) error\n" +
                "\tBaz(string) stream<int32, bool>\n" +
                "}\n" +
                "\n" +
                "error (" +
                "\tSomeError() {\"en\": \"error description\"}\n" +
                ")";
    }

    @Nullable
    @Override
    public Map<String, TextAttributesKey> getAdditionalHighlightingTagToDescriptorMap() {
        return null;
    }

    @NotNull
    @Override
    public AttributesDescriptor[] getAttributeDescriptors() {
        AttributesDescriptor[] result = new AttributesDescriptor[attributes.size()];
        int i = 0;
        for (Map.Entry<String, TextAttributesKey> entry : attributes.entrySet()) {
            result[i++] = new AttributesDescriptor(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @NotNull
    @Override
    public ColorDescriptor[] getColorDescriptors() {
        return ColorDescriptor.EMPTY_ARRAY;
    }

    @NotNull
    @Override
    public String getDisplayName() {
        return "VDL";
    }
}