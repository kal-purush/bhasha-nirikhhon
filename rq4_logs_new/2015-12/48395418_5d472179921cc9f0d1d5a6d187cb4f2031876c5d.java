package io.v.vdl;

import com.intellij.lang.BracePair;
import com.intellij.lang.PairedBraceMatcher;
import com.intellij.psi.PsiFile;
import com.intellij.psi.tree.IElementType;
import io.v.vdl.psi.VdlTypes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class VdlBraceMatcher implements PairedBraceMatcher {
    private static final BracePair[] PAIRS = new BracePair[]{
            new BracePair(VdlTypes.LBRACE, VdlTypes.RBRACE, true),
            new BracePair(VdlTypes.LPAREN, VdlTypes.RPAREN, false),
            new BracePair(VdlTypes.LBRACK, VdlTypes.RBRACK, false),
    };

    @NotNull
    @Override
    public BracePair[] getPairs() {
        return PAIRS;
    }

    @Override
    public boolean isPairedBracesAllowedBeforeType(@NotNull IElementType lbraceType, @Nullable IElementType type) {
        return VdlParserDefinition.COMMENTS.contains(type)
                || VdlParserDefinition.WHITESPACES.contains(type)
                || type == VdlTypes.SEMICOLON
                || type == VdlTypes.COMMA
                || type == VdlTypes.RPAREN
                || type == VdlTypes.RBRACK
                || type == VdlTypes.RBRACE
                || null == type;
    }

    @Override
    public int getCodeConstructStart(PsiFile file, int openingBraceOffset) {
        return openingBraceOffset;
    }
}