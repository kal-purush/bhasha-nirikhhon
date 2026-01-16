package seedu.duke.commands;

import seedu.duke.DukeException;
import seedu.duke.common.Messages;
import seedu.duke.model.ListType;
import seedu.duke.model.Model;
import seedu.duke.model.item.Expense;
import seedu.duke.model.itemlist.ExpenseList;
import seedu.duke.parser.Parser;
import seedu.duke.ui.Ui;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Deletes an expense item identified by its index in the expense list.
 */
public class DeleteExpenseCommand extends Command {

    private int index;
    private String currency;
    private LocalDate date;
    private boolean hasCurrency = false;
    private boolean hasDate = false;
    private boolean hasIndex = false;

    /**
     * Constructor for deleting expense items using index.
     *
     * @param index the index of the expense item to be deleted
     */
    public DeleteExpenseCommand(int index) {
        assert index > 0: "Expense item number should be greater than 0";
        this.hasCurrency = false;
        this.hasCurrency = false;
        this.hasIndex = true;
        this.index = index;
    }

    /**
     * Constructor for deleting all the expense items with a specific currency or date.
     *
     * @param argumentString A string containing "currency/CURRENCY" or "date/yyyy-MM-dd"
     * @throws DukeException when currency or date arguments are invalid.
     */
    public DeleteExpenseCommand(String argumentString) throws DukeException {
        HashMap<String, String> argumentsMap = Parser.getArgumentsFromRegex(argumentString, Parser.ARGUMENT_REGEX);
        if (argumentsMap.keySet().size() == 0) {
            throw new DukeException(Messages.EXCEPTION_INVALID_ARGUMENTS);
        }
        for (String argument : argumentsMap.keySet()) {
            if (!argument.equals("currency") && !argument.equals("date")) {
                throw new DukeException(Messages.EXCEPTION_INVALID_ARGUMENTS);
            }
        }
        if (argumentsMap.containsKey("currency")) {
            this.hasCurrency = true;
            currency = argumentsMap.get("currency");
            if (currency.equals("")) {
                throw new DukeException(Messages.EXCEPTION_INVALID_ARGUMENTS);
            }
        }
        if (argumentsMap.containsKey("date")) {
            this.hasDate = true;
            String dateString = argumentsMap.get("date");
            try {
                date = LocalDate.parse(dateString);
            } catch (DateTimeParseException e) {
                throw new DukeException(Messages.EXCEPTION_WRONG_DATE_FORMAT);
            }
        }
    }

    /**
     * Executes the command.
     *
     * @param model Model representing program data in memory.
     */
    @Override
    public void execute(Model model) {
        ExpenseList expenses = (ExpenseList) model.getList(ListType.EXPENSE_LIST);
        HashSet<Expense> expensesDeleted = new HashSet<>();
        if (hasCurrency) {
            for (int i = 0; i < expenses.size(); i++) {
                if (expenses.get(i).getCurrency().equals(currency)) {
                    expensesDeleted.add(expenses.get(i));
                }
            }
        }
        if (hasDate) {
            for (int i = 0; i < expenses.size(); i++) {
                if (expenses.get(i).getDate().toString().equals(date.toString())) {
                    expensesDeleted.add(expenses.get(i));
                }
            }
        }
        if (hasIndex) {
            expensesDeleted.add(expenses.get(index - 1));
        }
        if (expensesDeleted.size() == 0) {
            Ui.dukePrint(Messages.MESSAGE_NO_EXPENSE_DELETED);
        } else {
            expenses.deleteExpenses(expensesDeleted);
        }
    }
}