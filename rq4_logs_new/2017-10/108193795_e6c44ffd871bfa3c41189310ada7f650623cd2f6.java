package au.com.anz.calculator;

import au.com.anz.calculator.exception.InvalidInputException;
import au.com.anz.calculator.operation.CalculatorOperation;
import au.com.anz.calculator.au.com.anz.calculator.factory.CalculatorOperationFactory;

/**
 * Calculator App
 *
 */
public class CalculatorApp
{
    public static void main( String[] args )
    {
        try {
            CalculatorOperation operation = CalculatorOperationFactory.getCalculatorOperation(args);
            System.out.print(operation.calculate());
        } catch (InvalidInputException e) {
            System.out.print(e.getMessage());
        }
    }
}