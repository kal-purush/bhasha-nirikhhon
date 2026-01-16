package org.jivesoftware.openfire;

import java.io.PrintStream;
import java.io.PrintWriter;

public class DuplicateRegistrationException extends RuntimeException {
	private static final long serialVersionUID = 1L;

    private Throwable nestedThrowable = null;

    public DuplicateRegistrationException() {
        super();
    }

    public DuplicateRegistrationException(String msg) {
        super(msg);
    }

    public DuplicateRegistrationException(Throwable nestedThrowable) {
        this.nestedThrowable = nestedThrowable;
    }

    public DuplicateRegistrationException(String msg, Throwable nestedThrowable) {
        super(msg);
        this.nestedThrowable = nestedThrowable;
    }

    @Override
	public void printStackTrace() {
        super.printStackTrace();
        if (nestedThrowable != null) {
            nestedThrowable.printStackTrace();
        }
    }

    @Override
	public void printStackTrace(PrintStream ps) {
        super.printStackTrace(ps);
        if (nestedThrowable != null) {
            nestedThrowable.printStackTrace(ps);
        }
    }

    @Override
	public void printStackTrace(PrintWriter pw) {
        super.printStackTrace(pw);
        if (nestedThrowable != null) {
            nestedThrowable.printStackTrace(pw);
        }
    }
}