package myrmi.server;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Message implements Serializable
{
    int objectKey;
    String methodName;
    Class<?>[] argTypes;
    Object[] args;
    Object result;

    public Message(int objectKey, String methodName, Object... args)
    {
        this.objectKey = objectKey;
        this.methodName = methodName;
        this.args = args;
        List<Class<?>> argList = Arrays.stream(args).map(Object::getClass).collect(Collectors.toList());
        argTypes = new Class<?>[argList.size()];
        argList.toArray(argTypes);
    }

    public Object getResult()
    {
        return result;
    }

    public void setResult(Object result)
    {
        this.result = result;
    }
}