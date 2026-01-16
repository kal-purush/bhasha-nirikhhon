package io.v.vdl;

import com.intellij.lexer.Lexer;
import com.intellij.openapi.editor.colors.TextAttributesKey;
import com.intellij.openapi.fileTypes.SyntaxHighlighterBase;
import com.intellij.psi.TokenType;
import com.intellij.psi.tree.IElementType;
import io.v.vdl.psi.VdlTypes;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

import static io.v.vdl.VdlSyntaxHighlightingColors.*;

public class VdlSyntaxHighlighter extends SyntaxHighlighterBase {
    private static final Map<IElementType, TextAttributesKey> ATTRIBUTES = new HashMap<IElementType, TextAttributesKey>();

    static {
        fillMap(ATTRIBUTES, LINE_COMMENT, VdlParserDefinition.LINE_COMMENT);
        fillMap(ATTRIBUTES, BLOCK_COMMENT, VdlParserDefinition.MULTILINE_COMMENT);
        fillMap(ATTRIBUTES, PARENTHESES, VdlTypes.LPAREN, VdlTypes.RPAREN);
        fillMap(ATTRIBUTES, BRACES, VdlTypes.LBRACE, VdlTypes.RBRACE);
        fillMap(ATTRIBUTES, BRACKETS, VdlTypes.LBRACK, VdlTypes.RBRACK);
        fillMap(ATTRIBUTES, BAD_CHARACTER, TokenType.BAD_CHARACTER);
        fillMap(ATTRIBUTES, IDENTIFIER, VdlTypes.IDENTIFIER);
        fillMap(ATTRIBUTES, DOT, VdlTypes.DOT, VdlTypes.TRIPLE_DOT);
        fillMap(ATTRIBUTES, COLON, VdlTypes.COLON);
        fillMap(ATTRIBUTES, SEMICOLON, VdlTypes.SEMICOLON);
        fillMap(ATTRIBUTES, COMMA, VdlTypes.COMMA);
        fillMap(ATTRIBUTES, VdlParserDefinition.OPERATORS, OPERATOR);
        fillMap(ATTRIBUTES, VdlParserDefinition.KEYWORDS, KEYWORD);
        fillMap(ATTRIBUTES, VdlParserDefinition.NUMBERS, NUMBER);
        fillMap(ATTRIBUTES, VdlParserDefinition.STRING_LITERALS, STRING);
    }

    @NotNull
    public Lexer getHighlightingLexer() {
        return new VdlLexer();
    }

    @NotNull
    public TextAttributesKey[] getTokenHighlights(IElementType tokenType) {
        return pack(ATTRIBUTES.get(tokenType));
    }
}