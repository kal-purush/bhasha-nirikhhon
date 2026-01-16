package php4java.Impl;

import php4java.Interfaces.*;
import java.lang.reflect.*;

public class PhpInstance implements IPhp
{
    public PhpInstance(Object obj) throws Exception
    {
        if (obj.getClass().getName() != "php4java.Native.Php")
            throw new Exception();
        
        // Try to init new PHP interpreter
        obj.getClass().getDeclaredMethod("init").invoke(obj);
        _php = obj;
    }

    @Override
    public IPhpVal execString(String code) throws php4java.Php4JavaException
    {
        try
        {
            var result = _php.getClass().getDeclaredMethod("__eval", new Class[]{String.class}).invoke(_php, "eval('" + code + "');");
            return new PhpVal(result);
        }
        catch(InvocationTargetException exc)
        {
            throw new php4java.Php4JavaException(exc.getCause().toString());
        }
        catch(IllegalAccessException | NoSuchMethodException exc)
        {
            throw new php4java.Php4JavaException(exc.toString());
        }
    }

    protected Object _php;
}