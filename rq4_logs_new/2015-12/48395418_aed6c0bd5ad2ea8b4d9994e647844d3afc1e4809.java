package io.v.vdl.psi;

import com.intellij.psi.stubs.IStubElementType;
import com.intellij.psi.stubs.StubElement;
import com.intellij.util.io.StringRef;

public class VdlFieldDefinitionStub extends VdlNamedStub<VdlFieldDefinition> {
    public VdlFieldDefinitionStub(StubElement parent, IStubElementType elementType, StringRef name, boolean isPublic) {
        super(parent, elementType, name, isPublic);
    }

    public VdlFieldDefinitionStub(StubElement parent, IStubElementType elementType, String name, boolean isPublic) {
        super(parent, elementType, name, isPublic);
    }

}