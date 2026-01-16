package io.v.vdl;

import com.intellij.psi.PsiFile;
import com.intellij.psi.StubBuilder;
import com.intellij.psi.stubs.DefaultStubBuilder;
import com.intellij.psi.stubs.IndexSink;
import com.intellij.psi.stubs.StubElement;
import com.intellij.psi.tree.IStubFileElementType;
import io.v.vdl.psi.VdlFile;
import io.v.vdl.psi.VdlFileStub;
import org.jetbrains.annotations.NotNull;

public class VdlFileElementType extends IStubFileElementType<VdlFileStub> {
    public static final IStubFileElementType INSTANCE = new VdlFileElementType();
    public static final int VERSION = 1;

    private VdlFileElementType() {
        super("VDL_FILE", VdlLanguage.INSTANCE);
    }

    @Override
    public int getStubVersion() {
        return VERSION;
    }

    @NotNull
    @Override
    public StubBuilder getBuilder() {
        return new DefaultStubBuilder() {
            @NotNull
            @Override
            protected StubElement createStubForFile(@NotNull PsiFile file) {
                if (file instanceof VdlFile) {
                    return new VdlFileStub((VdlFile)file);
                }
                return super.createStubForFile(file);
            }
        };
    }

    @Override
    public void indexStub(@NotNull VdlFileStub stub, @NotNull IndexSink sink) {
        super.indexStub(stub, sink);
    }

    @NotNull
    @Override
    public String getExternalId() {
        return "vdl.FILE";
    }
}