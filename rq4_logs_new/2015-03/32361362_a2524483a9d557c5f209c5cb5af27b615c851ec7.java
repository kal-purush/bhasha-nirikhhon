package ass.prochka6;

import de.odysseus.el.util.SimpleContext;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

/**
 * @author Kamil Prochazka
 */
public class Main {

    public static void main(String[] args) {
        ExpressionFactory expressionFactory = ExpressionFactory.newInstance();
        SimpleContext context = new SimpleContext();
        ValueExpression ve = expressionFactory.createValueExpression(context, "${5*(60-10)}", Integer.class);
        Object value = ve.getValue(context);
        System.out.println(value);
    }

}