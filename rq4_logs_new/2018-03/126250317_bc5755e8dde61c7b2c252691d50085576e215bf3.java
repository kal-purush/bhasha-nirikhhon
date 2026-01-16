package br.utfpr.tdd.ex1;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author andreendo
 */
public class ProcessadorDeNotasIntegrationTest {
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Test
    public void testIntegracao() throws IOException {
        File tempFolder = folder.newFolder();
        String outputFile = tempFolder.getAbsolutePath() + "/saida.csv";
        
        ProcessadorDeNotas p = new ProcessadorDeNotas();
        p.processar("./res/csv03.csv", outputFile);

        
        File csvFileCreated = new File(outputFile);
        assertTrue( csvFileCreated.exists() );
        
        List<String> lines = FileUtils.readLines(csvFileCreated, "UTF-8");
 
        assertEquals(6, lines.size());
        
        List<String> expectedLines = Arrays.asList(
                "RA,Nome,NF,Situacao",
                "1234,Joao Da Silva,5.4,reprovado por nota",
                "4567,Maria Souza,8.9,aprovado",
                "4568,Maria Souza Prima,5.0,reprovado por nota e falta",
                "456448,Souza Prima,7.4,reprovado por falta",
                "488848,Souza Prima Segunda,5.4,reprovado por nota"
        );
        
        for (int i = 0; i < expectedLines.size(); i++) {
            assertEquals(expectedLines.get(i), lines.get(i));
        }
    }
}