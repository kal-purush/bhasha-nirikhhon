package software.sebastian.oposiciones.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class TokenizerPythonCaller {

    /**
     * Llama al script Python count_tokens.py para truncar texto según tokens.
     * 
     * @param modelo Nombre del modelo para tokenización (ej: "text-embedding-3-large")
     * @param maxTokens Número máximo de tokens permitido
     * @param texto Texto a tokenizar y truncar
     * @return Texto truncado a maxTokens tokens
     * @throws Exception Si la ejecución falla o la salida es incorrecta
     */
    public String truncarTextoPorTokens(String modelo, int maxTokens, String texto) throws Exception {
        String scriptPath = "./bin/count_tokens.py";

        ProcessBuilder pb = new ProcessBuilder(
            "python3", scriptPath, modelo, String.valueOf(maxTokens), texto
        );
        pb.redirectErrorStream(true);
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String totalTokensLine = reader.readLine();
            String truncatedText = reader.lines().collect(Collectors.joining("\n"));

            int totalTokens = Integer.parseInt(totalTokensLine.trim());
            if (totalTokens <= maxTokens) {
                return truncatedText;
            } else {
                return truncatedText;
            }
        } finally {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Error al ejecutar el script Python. Código de salida: " + exitCode);
            }
        }
    }
}