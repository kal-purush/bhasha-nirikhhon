package io.v.vdl;

import com.intellij.openapi.fileTypes.LanguageFileType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;

public class VdlFileType extends LanguageFileType {
    public static final VdlFileType INSTANCE = new VdlFileType();

    protected VdlFileType() {
        super(VdlLanguage.INSTANCE);
    }

    @NotNull
    @Override
    public String getName() {
        return "VDL";
    }

    @NotNull
    @Override
    public String getDescription() {
        return "Vanadium Definition Language file";
    }

    @NotNull
    @Override
    public String getDefaultExtension() {
        return "vdl";
    }

    @Nullable
    @Override
    public Icon getIcon() {
        return VdlIcons.FILE;
    }
}