package person.liuxx.read.exception;

/**
 * @author 刘湘湘
 * @version 1.0.0<br>
 *          创建时间：2018年8月15日 下午9:08:33
 * @since 1.0.0
 */
public class BookParseException extends RuntimeException
{
    /**
     * 
     */
    private static final long serialVersionUID = 4788975508784564400L;

    public BookParseException(String message)
    {
        super(message);
    }

    public BookParseException(String message, Throwable cause)
    {
        super(message, cause);
    }
}