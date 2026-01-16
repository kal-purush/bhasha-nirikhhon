package seedu.smartlib.logic.commands;

import seedu.smartlib.logic.commands.exceptions.CommandException;
import seedu.smartlib.model.Model;

import static seedu.smartlib.commons.util.CollectionUtil.requireAllNonNull;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_BOOK;

public class BorrowCommand extends Command {

    public static final String COMMAND_WORD = "borrow";
    public static final String MESSAGE_NOT_IMPLEMENTED_YET = "Remark command not implemented yet";
    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Edits the remark of the person identified "
            + "by the index number used in the last person listing. "
            + "Existing remark will be overwritten by the input.\n"
            + "Parameters: INDEX (must be a positive integer) "
            + PREFIX_BOOK + "[REMARK]\n"
            + "Example: " + COMMAND_WORD + " 1 "
            + PREFIX_BOOK + "Likes to swim.";

    private final int bid;
    private final int rid;

    public BorrowCommand(int bid, int rid) {
        requireAllNonNull(bid, rid);
        this.bid = bid;
        this.rid = rid;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        throw new CommandException(MESSAGE_NOT_IMPLEMENTED_YET);
    }

    @Override
    public boolean equals(Object other) {
        // short circuit if same object
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof BorrowCommand)) {
            return false;
        }

        // state check
        BorrowCommand e = (BorrowCommand) other;
        return bid == e.bid
                && rid == e.rid;
    }
}