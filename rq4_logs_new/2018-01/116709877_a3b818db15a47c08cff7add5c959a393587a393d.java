package pl.kostrowski.nauka.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.kostrowski.nauka.timecalculator.TimeCalculator;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class CalculationInvocationHandler implements InvocationHandler {

    private Logger LOGGER = LoggerFactory.getLogger(CalculationInvocationHandler.class);

    private Map<String, Method> methods = new HashMap<>();


    //chcę mieć metodę dostępną po kluczu zgodnym z wyborem użytkownika
    //w menu tekstowym
    public CalculationInvocationHandler() throws NoSuchMethodException {
        this.methods.put("1", TimeCalculator.class.getMethod("getDateDifference",String.class,String.class));
        this.methods.put("2", TimeCalculator.class.getMethod("getTimeDifference",String.class,String.class));
    }

    public Map<String, Method> getMethods() {
        return methods;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        return null;
    }

}