package cooper.command;

import cooper.exceptions.InvalidAccessException;
import cooper.finance.FinanceManager;
import cooper.resources.ResourcesManager;
import cooper.storage.StorageManager;
import cooper.ui.Ui;
import cooper.verification.SignInDetails;
import cooper.verification.UserRole;


public class GenerateCommand extends Command {

    private final String documentToGenerate;

    public GenerateCommand(String documentToGenerate) {
        this.documentToGenerate = documentToGenerate;
    }

    @Override
    public void execute(SignInDetails signInDetails, ResourcesManager resourcesManager, StorageManager storageManager)
            throws InvalidAccessException {
        UserRole userRole = signInDetails.getUserRole();
        FinanceManager financeManager = resourcesManager.getFinanceManager(userRole);

        if (financeManager == null) {
            Ui.printAdminHelp();
            Ui.printGeneralHelp();
            throw new InvalidAccessException();
        }

        if (documentToGenerate.equals("bs")) {
            financeManager.generateBalanceSheetAsPdf();
        } else if (documentToGenerate.equals("cf")) {
            int i;
        }
    }
}