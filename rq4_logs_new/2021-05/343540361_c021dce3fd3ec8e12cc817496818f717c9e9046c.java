package problem65;

public final class ValidNumber {

    private enum State {
        START,
        SIGN_BEGINNING,
        DIGITS_INT,
        DOT,
        DIGITS_DECIMAL,
        EXP,
        SIGN_EXP,
        DIGITS_EXP
    }

    public boolean isNumber(final String s) {
        State state = State.START;
        boolean possibleValidDeci = false;

        for (int i = 0; i < s.length(); i++) {
            var c = s.charAt(i);

            if ('0' <= c && c <= '9') {
                if (state == State.START || state == State.SIGN_BEGINNING || state == State.DIGITS_INT) {
                    state = State.DIGITS_INT;
                    possibleValidDeci = true;
                } else if (state == State.DOT || state == State.DIGITS_DECIMAL) {
                    state = State.DIGITS_DECIMAL;
                    possibleValidDeci = true;
                } else {
                    state = State.DIGITS_EXP;
                }
            } else if (c == '+' || c == '-') {
                if (state != State.START && state != State.EXP)
                    return false;
                state = state == State.START ? State.SIGN_BEGINNING : State.SIGN_EXP;
            } else if (c == '.') {
                if (state != State.START && state != State.SIGN_BEGINNING && state != State.DIGITS_INT)
                    return false;
                state = State.DOT;
            } else if (c == 'e' || c == 'E') {
                if ((state != State.DOT && state != State.DIGITS_INT && state != State.DIGITS_DECIMAL) || (state == State.DOT && !possibleValidDeci))
                    return false;
                state = State.EXP;
            } else {
                return false;
            }
        }

        return (state != State.DOT || possibleValidDeci) && state != State.SIGN_BEGINNING && state != State.EXP && state != State.SIGN_EXP;
    }

}