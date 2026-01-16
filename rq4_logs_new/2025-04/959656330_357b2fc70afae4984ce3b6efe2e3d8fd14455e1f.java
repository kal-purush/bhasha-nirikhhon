/*
 * Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.np.compilerplugin;

import io.ballerina.compiler.api.SemanticModel;
import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.TypeDefinitionSymbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeReferenceTypeSymbol;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.api.symbols.UnionTypeSymbol;
import io.ballerina.compiler.syntax.tree.FunctionArgumentNode;
import io.ballerina.compiler.syntax.tree.ModulePartNode;
import io.ballerina.compiler.syntax.tree.NaturalExpressionNode;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.ParenthesizedArgList;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.projects.Document;
import io.ballerina.projects.Module;
import io.ballerina.projects.ModuleId;
import io.ballerina.projects.Package;
import io.ballerina.projects.plugins.AnalysisTask;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.DiagnosticFactory;
import io.ballerina.tools.diagnostics.DiagnosticInfo;
import io.ballerina.tools.diagnostics.Location;

import java.util.Optional;

import static io.ballerina.lib.np.compilerplugin.Commons.MODULE_NAME;
import static io.ballerina.lib.np.compilerplugin.Commons.findNPModule;

/**
 * Natural programming function signature validator.
 *
 * @since 0.3.0
 */
public class NaturalExpressionValidator implements AnalysisTask<SyntaxNodeAnalysisContext> {
    private final CodeModifier.AnalysisData analysisData;
    private static final String MODEL_TYPE = "Model";
    private Optional<TypeSymbol> modelType = Optional.empty();

    NaturalExpressionValidator(CodeModifier.AnalysisData analysisData) {
        this.analysisData = analysisData;
    }

    @Override
    public void perform(SyntaxNodeAnalysisContext ctx) {
        SemanticModel semanticModel = ctx.semanticModel();
        TypeSymbol jsonType = semanticModel.types().JSON;

        Node node = ctx.node();

        if (node instanceof ModulePartNode rootNode) {
            Optional<ModuleSymbol> npModule = findNPModule(semanticModel, rootNode);
            if (npModule.isEmpty()) {
                this.modelType = Optional.empty();
                return;
            }

            ModuleSymbol npModuleSymbol = npModule.get();
            this.modelType = Optional.of(((TypeDefinitionSymbol) npModuleSymbol.allSymbols().stream()
                    .filter(
                            symbol -> symbol instanceof TypeDefinitionSymbol typeDefinitionSymbol &&
                                    typeDefinitionSymbol.moduleQualifiedName().equals(
                                            String.format("%s:%s", MODULE_NAME, MODEL_TYPE)))
                    .findFirst()
                    .get()).typeDescriptor());
            return;
        }

//        // Using the `modelType` to also track if natural expressions have been rewritten.
//        // Ideally should happen on the natural expression itself, but then we may need several plugin iterations.
//        if (this.modelType.isEmpty()) {
//            return;
//        }

        Package currentPackage = ctx.currentPackage();
        ModuleId moduleId = ctx.moduleId();
        Module module = currentPackage.module(moduleId);
        Document document = module.document(ctx.documentId());

        validateNaturalExpression(semanticModel, document, (NaturalExpressionNode) ctx.node(), jsonType, ctx);
    }

    private void validateNaturalExpression(SemanticModel semanticModel,
                                           Document document,
                                           NaturalExpressionNode naturalExpressionNode,
                                           TypeSymbol jsonType, SyntaxNodeAnalysisContext ctx) {
        validateArguments(ctx, semanticModel, naturalExpressionNode.parenthesizedArgList());
        validateExpectedType(naturalExpressionNode,
                semanticModel.expectedType(document, naturalExpressionNode.lineRange().startLine()), jsonType, ctx);
    }

    private void validateArguments(SyntaxNodeAnalysisContext ctx,
                                   SemanticModel semanticModel,
                                   Optional<ParenthesizedArgList> parenthesizedArgListOptional) {
        if (parenthesizedArgListOptional.isEmpty()) {
            return;
        }

        ParenthesizedArgList parenthesizedArgList = parenthesizedArgListOptional.get();
        SeparatedNodeList<FunctionArgumentNode> argList = parenthesizedArgList.arguments();
        int argListSize = argList.size();

        if (argListSize == 0) {
            return;
        }

        if (argListSize > 1) {
            reportDiagnostic(ctx, parenthesizedArgList.location(), DiagnosticCode.UNEXPECTED_ARGUMENTS);
        }

        if (this.modelType.isEmpty()) {
            return;
        }

        FunctionArgumentNode arg0 = argList.get(0);
        Optional<TypeSymbol> argType = semanticModel.typeOf(arg0.lineRange());
        if (argType.isEmpty()) {
            return;
        }

        if (!argType.get().subtypeOf(this.modelType.get())) {
            reportDiagnostic(ctx, arg0.location(), DiagnosticCode.EXPECTED_A_SUBTYPE_OF_NP_MODEL);
        }
    }

    private void validateExpectedType(NaturalExpressionNode naturalExpressionNode,
                                      Optional<TypeSymbol> expectedType,
                                      TypeSymbol jsonType,
                                      SyntaxNodeAnalysisContext ctx) {
        Location location = naturalExpressionNode.location();
        TypeSymbol expectedTypeSymbol = expectedType.get();
        if (expectedTypeSymbol instanceof TypeReferenceTypeSymbol typeReferenceTypeSymbol) {
            expectedTypeSymbol = typeReferenceTypeSymbol.typeDescriptor();
        }

        if (!(expectedTypeSymbol instanceof UnionTypeSymbol unionTypeSymbol)) {
            if (expectedTypeSymbol.typeKind() != TypeDescKind.ERROR && expectedTypeSymbol.subtypeOf(jsonType)) {
                reportDiagnostic(ctx, location, DiagnosticCode.NON_JSON_EXPECTED_TYPE_NOT_YET_SUPPORTED);
            }
            return;
        }

        for (TypeSymbol memberTypeDescriptor : unionTypeSymbol.memberTypeDescriptors()) {
            if (memberTypeDescriptor instanceof TypeReferenceTypeSymbol typeReferenceTypeSymbol) {
                memberTypeDescriptor = typeReferenceTypeSymbol.typeDescriptor();
            }

            if (memberTypeDescriptor.typeKind() != TypeDescKind.ERROR && !memberTypeDescriptor.subtypeOf(jsonType)) {
                reportDiagnostic(ctx, location, DiagnosticCode.NON_JSON_EXPECTED_TYPE_NOT_YET_SUPPORTED);
            }
        }
    }

    private void reportDiagnostic(SyntaxNodeAnalysisContext ctx, Location location,
                                  DiagnosticCode diagnosticsCode) {
        DiagnosticInfo diagnosticInfo = new DiagnosticInfo(diagnosticsCode.getCode(),
                diagnosticsCode.getMessage(), diagnosticsCode.getSeverity());
        this.analysisData.analysisTaskErrored = true;
        ctx.reportDiagnostic(DiagnosticFactory.createDiagnostic(diagnosticInfo, location));
    }
}