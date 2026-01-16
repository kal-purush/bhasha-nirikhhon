package com.tinkoff.edu.app.repository;

import com.tinkoff.edu.app.model.LoanRequest;
import com.tinkoff.edu.app.model.LoanResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.*;

public class FileLoanCalcRepository implements LoanCalcRepository {
    private final Map<UUID, LoanRequest> loanRequests = new HashMap<>();
    private final Map<UUID, LoanResponse> loanResponses = new HashMap<>();
    private final String loanRequestFile = "LoanRequest.txt";
    private final String loanResponseFile = "LoanResponse.txt";
    private String filePath = "./src/main/java/com/tinkoff/edu/app/repository/";
    private LoanResponse loanResponse;

    @Override
    public boolean save(LoanRequest loanRequest, LoanResponse loanResponse) {

        if (loanRequest == null)
            throw new NullPointerException("Данные по заявке отсутствуют");

        if (loanResponse == null)
            throw new NullPointerException("Ответ по заявке не был сформирован");

        writeLineToFile(loanRequestFile, loanRequest.toString());
        writeLineToFile(loanResponseFile, loanResponse.toString());

        return true;
    }

    void writeLineToFile(String fileName, String textLine) {
        textLine += System.lineSeparator();

        try {
            Path path = Path.of(filePath + fileName);
            Files.writeString(path, textLine, CREATE, APPEND, WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}