package cooper.command;

import cooper.exceptions.EmptyFinancialStatementException;
import cooper.exceptions.InvalidAccessException;
import cooper.finance.FinanceCommand;
import cooper.finance.FinanceManager;
import cooper.resources.ResourcesManager;
import cooper.storage.StorageManager;
import cooper.ui.Ui;
import cooper.ui.FinanceUi;
import cooper.verification.SignInDetails;
import cooper.verification.UserRole;

public class ProjectionCommand extends Command {

    public int years;
    public int oldIndex = 9;
    public FinanceCommand financeFlag;

    public ProjectionCommand(int years, FinanceCommand financeFlag) {
        super();
        this.years = years;
        this.financeFlag = financeFlag;
    }

    @Override
    public void execute(SignInDetails signInDetails, ResourcesManager resourcesManager,
                        StorageManager storageManager) throws InvalidAccessException, EmptyFinancialStatementException {
        UserRole userRole = signInDetails.getUserRole();
        FinanceManager financeManager = resourcesManager.getFinanceManager(userRole);
        if (financeManager == null) {
            Ui.printAdminHelp();
            Ui.printGeneralHelp();
            throw new InvalidAccessException();
        }

        if (financeFlag == FinanceCommand.IDLE) {
            FinanceUi.showPleaseSpecifyFinancialStatement();
        }

        boolean isEmptyCf = isEmptyFinancialStatement(financeManager.cooperCashFlowStatement.getCashFlowStatement());

        if (financeFlag == FinanceCommand.PROJ) {
            if (isEmptyCf) {
                throw new EmptyFinancialStatementException();
            }
            int newFreeCF = financeManager.calculateFreeCashFlow(
                    financeManager.cooperCashFlowStatement.getCashFlowStatement());
            int oldFreeCF = financeManager.cooperCashFlowStatement.cashFlowStatement.get(oldIndex);
            int differenceFreeCF = newFreeCF - oldFreeCF;
            double rateOfGrowth = (differenceFreeCF / (double) oldFreeCF) * 100.0;
            double finalGrowthValue = financeManager.createProjection(newFreeCF, rateOfGrowth, years);
            FinanceUi.printProjections(finalGrowthValue, financeManager.cooperProjection.getProjection());
            financeManager.cooperProjection.getProjection().clear();
        }
    }
}