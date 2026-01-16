package seedu.duke.model.itemlist;

import seedu.duke.DukeException;
import seedu.duke.common.Messages;
import seedu.duke.model.item.Expense;
import seedu.duke.model.item.TotalExpenseType;
import seedu.duke.ui.Ui;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;

import static java.time.DayOfWeek.MONDAY;
import static java.time.DayOfWeek.SUNDAY;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;
import static java.time.temporal.TemporalAdjusters.lastDayOfYear;
import static java.time.temporal.TemporalAdjusters.nextOrSame;
import static java.time.temporal.TemporalAdjusters.previousOrSame;


/**
 * Represents a list of expense items.
 */
public class ExpenseList extends ItemList<Expense> {

    public ExpenseList() {
        items = new ArrayList<>();
    }

    public ExpenseList(ArrayList<Expense> expenses) {
        super(expenses);
    }

    @Override
    public void addItem(Expense expense) {
        items.add(expense);
        Ui.dukePrint(Messages.MESSAGE_ADD_EXPENSE + expense.toString() + Messages.MESSAGE_STATUS_FIRST
                + items.size() + Messages.MESSAGE_EXPENSE_STATUS_LAST);
    }

    @Override
    public void listTask() {
        if (items.size() == 0) {
            Ui.dukePrint(Messages.MESSAGE_EMPTY_EXPENSE_LIST);
            return;
        }
        Ui.showLine();
        Ui.dukePrintMultiple(Messages.MESSAGE_EXPENSE_LIST);
        items.forEach(expense -> Ui.dukePrintMultiple(expense.toString()));
        try {
            Ui.dukePrintMultiple(Messages.MESSAGE_EXPENSE_DAY);
            HashMap<String, Double> dayMap = getTotalExpense(TotalExpenseType.DAY);
            for (String currency : dayMap.keySet()) {
                Ui.dukePrintMultiple(dayMap.get(currency) + " " + currency);
            }
            Ui.dukePrintMultiple(Messages.MESSAGE_EXPENSE_WEEK);
            HashMap<String, Double> weekMap = getTotalExpense(TotalExpenseType.WEEK);
            for (String currency : weekMap.keySet()) {
                Ui.dukePrintMultiple(weekMap.get(currency) + " " + currency);
            }
            Ui.dukePrintMultiple(Messages.MESSAGE_EXPENSE_MONTH);
            HashMap<String, Double> monthMap = getTotalExpense(TotalExpenseType.MONTH);
            for (String currency : monthMap.keySet()) {
                Ui.dukePrintMultiple(monthMap.get(currency) + " " + currency);
            }
            Ui.dukePrintMultiple(Messages.MESSAGE_EXPENSE_YEAR);
            HashMap<String, Double> yearMap = getTotalExpense(TotalExpenseType.YEAR);
            for (String currency : yearMap.keySet()) {
                Ui.dukePrintMultiple(yearMap.get(currency) + " " + currency);
            }
        } catch (DukeException e) {
            Ui.dukePrintMultiple(e.toString());
        }
    }

    /**
     * Gets the total value of expenses during a time period for each currency.
     *
     * @param totalExpenseType a {@code TotalExpenseType} value, i.e. DAY, WEEK, MONTH, or YEAR
     * @return a hashmap where the key is the currency and value is the total value of expenses for the currency.
     * @throws DukeException when the {@code totalExpenseType} is not valid.
     */
    public HashMap<String, Double> getTotalExpense(TotalExpenseType totalExpenseType) throws DukeException {
        ArrayList<Expense> selectedItems = new ArrayList<>();
        HashMap<String, Double> totalMap = new HashMap<>();
        try {
            selectedItems = getExpenseItems(totalExpenseType);
        } catch (DukeException e) {
            throw new DukeException(e.toString());
        }
        for (Expense item : selectedItems) {
            if (!totalMap.containsKey(item.getCurrency())) {
                // Currency does not exist in the hashmap
                totalMap.put(item.getCurrency(), item.getValue());
            } else {
                totalMap.put(item.getCurrency(), totalMap.get(item.getValue()) + item.getValue());
            }
        }
        return totalMap;
    }

    /**
     * Retrieves all the expense items in the date range.
     *
     * @param totalExpenseType a {@code TotalExpenseType} value, i.e. DAY, WEEK, MONTH, or YEAR
     * @return an ArrayList of expense items in the date range.
     * @throws DukeException when the argument {@code totalExpenseType} is invalid.
     */
    public ArrayList<Expense> getExpenseItems(TotalExpenseType totalExpenseType) throws DukeException {
        LocalDate today = LocalDate.now();
        ArrayList<Expense> resultExpenses = new ArrayList<>();
        switch (totalExpenseType) {
        case DAY:
            resultExpenses = new ArrayList<>();
            for (Expense item : items) {
                if (item.getDate().compareTo(today) == 0) {
                    resultExpenses.add(item);
                }
            }
            break;
        case WEEK:
            resultExpenses = new ArrayList<>();
            LocalDate monday = today.with(previousOrSame(MONDAY));
            LocalDate sunday = today.with(nextOrSame(SUNDAY));
            for (Expense item : items) {
                if (item.getDate().compareTo(monday) >= 0 && item.getDate().compareTo(sunday) <= 0) {
                    resultExpenses.add(item);
                }
            }
            break;
        case MONTH:
            resultExpenses = new ArrayList<>();
            LocalDate monthBegin = today.withDayOfMonth(1);
            LocalDate monthEnd = today.plusMonths(1).withDayOfMonth(1).minusDays(1);
            for (Expense item : items) {
                if (item.getDate().compareTo(monthBegin) >= 0 && item.getDate().compareTo(monthEnd) <= 0) {
                    resultExpenses.add(item);
                }
            }
            break;
        case YEAR:
            resultExpenses = new ArrayList<>();
            LocalDate firstDay = today.with(firstDayOfYear());
            LocalDate lastDay = today.with(lastDayOfYear());
            for (Expense item : items) {
                if (item.getDate().compareTo(firstDay) >= 0 && item.getDate().compareTo(lastDay) <= 0) {
                    resultExpenses.add(item);
                }
            }
            break;
        default:
            throw new DukeException(Messages.EXCEPTION_EXPENSE_DATERANGE);
        }
        return resultExpenses;
    }
}