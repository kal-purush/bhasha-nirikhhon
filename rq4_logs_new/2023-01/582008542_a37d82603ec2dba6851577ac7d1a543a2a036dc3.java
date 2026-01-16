package by.teachmeskills.exceptions.accountingsystem;

import java.util.Date;

public class AccountingSystemDemo {
    public static void main(String[] args) throws WrongDocumentFormatException {
        Date date = new Date();
        Document[] documents = {
                new SupplyGoodsContract(date, "555abc1a2b", "type1", 100),
                new ContractWithEmployee(date, "456", date, "Employee name"),
                new FinancialInvoice(date, "555abc", 120, "1")};
        Register document = new Document();
        try {
            document.saveDocument(documents);
        } catch (WrongDocumentFormatException e) {
            System.out.println(e);
        }
        document.getDocumentInformation(documents[0]);
    }
}