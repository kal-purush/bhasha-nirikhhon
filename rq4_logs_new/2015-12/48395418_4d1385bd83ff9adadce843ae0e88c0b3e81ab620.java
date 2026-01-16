// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package io.v.vdl;

import com.intellij.codeInsight.completion.CompletionContributor;
import com.intellij.codeInsight.completion.CompletionType;
import com.intellij.codeInsight.lookup.AutoCompletionPolicy;
import com.intellij.openapi.project.DumbAware;
import com.intellij.patterns.CollectionPattern;
import com.intellij.patterns.ElementPattern;
import com.intellij.patterns.PsiElementPattern;
import com.intellij.patterns.PsiFilePattern;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiErrorElement;
import com.intellij.psi.tree.IElementType;
import com.intellij.psi.tree.TokenSet;
import io.v.vdl.psi.VdlBlock;
import io.v.vdl.psi.VdlExpression;
import io.v.vdl.psi.VdlFile;
import io.v.vdl.psi.VdlImportList;
import io.v.vdl.psi.VdlLeftHandExprList;
import io.v.vdl.psi.VdlSpecType;
import io.v.vdl.psi.VdlStatement;
import io.v.vdl.psi.VdlType;
import io.v.vdl.psi.VdlTypeReferenceExpression;
import io.v.vdl.psi.VdlTypes;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

import static com.intellij.patterns.PlatformPatterns.psiElement;
import static com.intellij.patterns.PlatformPatterns.psiFile;
import static com.intellij.patterns.StandardPatterns.collection;
import static com.intellij.patterns.StandardPatterns.not;
import static com.intellij.patterns.StandardPatterns.or;
import static com.intellij.patterns.StandardPatterns.string;

public class VdlKeywordCompletionContributor extends CompletionContributor implements DumbAware {
    private static final int KEYWORD_PRIORITY = 20;

    public VdlKeywordCompletionContributor() {
        extend(CompletionType.BASIC, packagePattern(), new VdlKeywordCompletionProvider(KEYWORD_PRIORITY,
                AutoCompletionPolicy.ALWAYS_AUTOCOMPLETE, "package"));
        extend(CompletionType.BASIC, importPattern(), new VdlKeywordCompletionProvider(KEYWORD_PRIORITY, "import"));
        extend(CompletionType.BASIC, topLevelPattern(), new VdlKeywordCompletionProvider(KEYWORD_PRIORITY, "const", "var", "func", "type", "error"));
        extend(CompletionType.BASIC, insideBlockPattern(VdlTypes.IDENTIFIER),
                new VdlKeywordCompletionProvider(KEYWORD_PRIORITY, "for", "const", "var", "return", "if", "switch", "go", "defer", "goto"));
    }

    private static ElementPattern<? extends PsiElement> typeDeclaration() {
        return psiElement(VdlTypes.IDENTIFIER)
                .withParent(psiElement(VdlTypeReferenceExpression.class).withParent(psiElement(VdlType.class).withParent(VdlSpecType.class)));
    }

    private static PsiElementPattern.Capture<PsiElement> insideBlockPattern(@NotNull IElementType tokenType) {
        return onStatementBeginning(tokenType)
                .withParent(psiElement(VdlExpression.class).withParent(psiElement(VdlLeftHandExprList.class).withParent(
                        psiElement(VdlStatement.class).inside(VdlBlock.class))));
    }

    private static PsiElementPattern.Capture<PsiElement> topLevelPattern() {
        return onStatementBeginning(VdlTypes.IDENTIFIER).withParent(psiElement(PsiErrorElement.class).withParent(VdlFileWithPackage()));
    }

    private static PsiElementPattern.Capture<PsiElement> importPattern() {
        return onStatementBeginning(VdlTypes.IDENTIFIER)
                .withParent(psiElement(PsiErrorElement.class).afterSiblingSkipping(psiElement().whitespace(),
                        psiElement(VdlImportList.class)));
    }

    private static PsiElementPattern.Capture<PsiElement> packagePattern() {
        return psiElement(VdlTypes.IDENTIFIER)
                .withParent(psiElement(PsiErrorElement.class).withParent(VdlFileWithoutPackage()).isFirstAcceptedChild(psiElement()));
    }

    private static PsiElementPattern.Capture<PsiElement> onStatementBeginning(@NotNull IElementType... tokenTypes) {
        return psiElement().withElementType(TokenSet.create(tokenTypes))
                .afterLeafSkipping(psiElement().whitespaceCommentEmptyOrError().withoutText(string().containsChars("\n")),
                        or(psiElement(VdlTypes.SEMICOLON), psiElement(VdlTypes.LBRACE), psiElement(VdlTypes.COLON),
                                psiElement().withText(string().containsChars("\n"))));
    }

    private static PsiFilePattern.Capture<VdlFile> VdlFileWithPackage() {
        CollectionPattern<PsiElement> collection = collection(PsiElement.class);
        CollectionPattern<PsiElement> packageIsFirst = collection.first(psiElement(VdlTypes.PACKAGE_CLAUSE));
        return psiFile(VdlFile.class).withChildren(collection.filter(not(psiElement().whitespaceCommentEmptyOrError()),
                packageIsFirst));
    }

    private static PsiFilePattern.Capture<VdlFile> VdlFileWithoutPackage() {
        CollectionPattern<PsiElement> collection = collection(PsiElement.class);
        ElementPattern<Collection<PsiElement>> emptyOrPackageIsNotFirst = or(collection.empty(),
                collection.first(not(psiElement(VdlTypes.PACKAGE_CLAUSE))));
        return psiFile(VdlFile.class).withChildren(collection.filter(not(psiElement().whitespaceCommentEmptyOrError()),
                emptyOrPackageIsNotFirst));
    }
}