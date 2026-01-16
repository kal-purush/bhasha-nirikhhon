package ru.itmo.icompiler;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.itmo.icompiler.syntax.ast.ASTNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.icompiler.common.Common.getTestFiles;

class ICompilerTest {

    static Stream<Arguments> provideGoodTestCases() throws IOException {
        return getTestFiles("src/test/resources/sem/good");
    }

    static Stream<Arguments> provideBadTestCases() throws IOException {
        return getTestFiles("src/test/resources/sem/bad");
    }

    @ParameterizedTest
    @MethodSource("provideGoodTestCases")
    void parseGood(Path file) throws FileNotFoundException {
        ICompiler compiler = new ICompiler(new File(file.toUri()));

        ASTNode n = compiler.parseProgram();
        compiler.checkSemantic();

        assertEquals(new ArrayList<>(), compiler.getCompilerErrors());
    }

    @ParameterizedTest
    @MethodSource("provideBadTestCases")
    void parseBad(Path file) throws FileNotFoundException {
        ICompiler compiler = new ICompiler(new File(file.toUri()));

        ASTNode n = compiler.parseProgram();
        compiler.checkSemantic();
        assertNotEquals(new ArrayList<>(), compiler.getCompilerErrors());
    }
}