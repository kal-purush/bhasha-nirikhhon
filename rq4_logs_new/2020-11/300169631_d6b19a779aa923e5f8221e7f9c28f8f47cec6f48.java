package seedu.duke.commands;

import seedu.duke.DukeException;
import seedu.duke.common.Messages;
import seedu.duke.model.ListType;
import seedu.duke.model.Model;
import seedu.duke.model.item.TotalExpenseType;
import seedu.duke.model.itemlist.ExpenseList;
import seedu.duke.parser.Parser;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Lists expense items in the expense list.
 */
public class ListExpenseCommand extends Command {

    private String currency;
    private LocalDate date;
    private TotalExpenseType totalExpenseType;
    private boolean hasCurrency;
    private boolean hasDate;
    private boolean hasTotalExpenseType;

    /**
     * Default constructor to list all the expense items in the expense list.
     */
    public ListExpenseCommand() {
        this.hasCurrency = false;
        this.hasDate = false;
        this.hasTotalExpenseType = false;
    }

    /**
     * Constructor to list all the expense items with the given currency in the expense list.
     *
     * @param currency the currency to be used to select expense items to be listed
     */
    public ListExpenseCommand(String currency) {
        this.currency = currency;
        this.hasCurrency = true;
        this.hasDate = false;
        this.hasTotalExpenseType = false;
    }

    /**
     * Constructor to list all the expense items with the given date in the expense list.
     *
     * @param date the date to be used to select expense items to be listed
     */
    public ListExpenseCommand(LocalDate date) {
        this.date = date;
        this.hasDate = true;
        this.hasCurrency = false;
        this.hasTotalExpenseType = false;
    }

    /**
     * Constructor to list all the expense items with the given {@code TotalExpenseType} in the expense list.
     *
     * @param totalExpenseType the {@code TotalExpenseType} to be used to select expense items to be listed
     */
    public ListExpenseCommand(TotalExpenseType totalExpenseType) {
        this.totalExpenseType = totalExpenseType;
        this.hasTotalExpenseType = true;
        this.hasCurrency = false;
        this.hasDate = false;
    }

    /**
     * Creates a ListExpenseCommand according to the input arguments.
     *
     * @param commandString A String containing the input arguments.
     * @return a ListExpenseCommand.
     * @throws DukeException when there are invalid arguments.
     */
    public static ListExpenseCommand createListExpenseCommand(String commandString) throws DukeException {
        ArrayList<String> allowedArguments = new ArrayList(Arrays.asList("currency", "date", "for"));
        if (commandString.length() == 0) {
            return new ListExpenseCommand();
        }
        HashMap<String, String> argumentsMap = Parser.getArgumentsFromRegex(commandString, Parser.ARGUMENT_REGEX);
        if (argumentsMap.keySet().size() != 1) {
            throw new DukeException(Messages.EXCEPTION_ONE_ARGUMENT_ONLY);
        }
        for (String key : argumentsMap.keySet()) {
            if (!allowedArguments.contains(key)) {
                throw new DukeException(Messages.EXCEPTION_INVALID_ARGUMENTS);
            }
        }
        String key = (String) argumentsMap.keySet().toArray()[0];
        if (key.equals("currency")) {
            String currency = argumentsMap.get(key);
            if (currency.equals("")) {
                throw new DukeException(Messages.EXCEPTION_EMPTY_CURRENCY);
            }
            return new ListExpenseCommand(currency);
        } else if (key.equals("date")) {
            String dateString = argumentsMap.get(key);
            LocalDate date;
            try {
                date = LocalDate.parse(dateString);
                return new ListExpenseCommand(date);
            } catch (DateTimeParseException e) {
                throw new DukeException(Messages.EXCEPTION_WRONG_DATE_FORMAT);
            }
        } else if (key.equals("for")) {
            String totalExpenseTypeString = argumentsMap.get(key).toUpperCase();
            try {
                TotalExpenseType totalExpenseType = TotalExpenseType.valueOf(totalExpenseTypeString);
                return new ListExpenseCommand(totalExpenseType);
            } catch (IllegalArgumentException e) {
                throw new DukeException(Messages.EXCEPTION_EXPENSE_DATERANGE);
            }
        }
        return new ListExpenseCommand();
    }

    /**
     * Executes the command.
     *
     * @param model Model representing program data in memory.
     * @throws DukeException when there are invalid arguments.
     */
    @Override
    public void execute(Model model) throws DukeException {
        ExpenseList expenses = (ExpenseList) model.getList(ListType.EXPENSE_LIST);
        if (hasCurrency) {
            if (currency.equals("")) {
                throw new DukeException(Messages.EXCEPTION_EMPTY_CURRENCY);
            }
            expenses.listExpense(currency);
        } else if (hasDate) {
            expenses.listExpense(date);
        } else if (hasTotalExpenseType) {
            expenses.listExpense(totalExpenseType);
        } else {
            expenses.listTask();
        }
    }
}