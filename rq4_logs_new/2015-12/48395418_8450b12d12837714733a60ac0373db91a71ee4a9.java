package io.v.vdl;

import com.intellij.lang.SmartEnterProcessorWithFixers;
import com.intellij.openapi.editor.Editor;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiFile;
import com.intellij.psi.util.PsiTreeUtil;
import io.v.vdl.psi.VdlBlock;
import io.v.vdl.psi.VdlSimpleStatement;
import io.v.vdl.psi.VdlStatement;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class VdlSmartEnterProcessor extends SmartEnterProcessorWithFixers {
    public VdlSmartEnterProcessor() {
        addEnterProcessors(new PlainEnterProcessor());
    }

    private static class PlainEnterProcessor extends FixEnterProcessor {
        @Nullable
        private static VdlBlock findBlock(@Nullable PsiElement element) {
            VdlStatement statement = PsiTreeUtil.getParentOfType(element, VdlStatement.class);
            if (statement instanceof VdlSimpleStatement && statement.getParent() instanceof VdlStatement) {
                statement = (VdlStatement)statement.getParent();
            }
            if (statement instanceof VdlBlock) return (VdlBlock)statement;
            return null;
        }

        @Override
        public boolean doEnter(PsiElement psiElement, PsiFile file, @NotNull Editor editor, boolean modified) {
            VdlBlock block = findBlock(psiElement);
            if (block != null) {
                int offset = block.getTextOffset() + 1;
                editor.getCaretModel().moveToOffset(offset);
            }
            plainEnter(editor);
            return true;
        }
    }
}