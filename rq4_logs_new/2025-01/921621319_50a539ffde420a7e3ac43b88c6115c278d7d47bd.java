package com.metamong.mt.global.mail;

import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 메일로 전달될 메시지를 포매팅하는 클래스
 * 
 * @author pgd
 */
public abstract class MailMessageFormatter {
    private final MessageFormat formatter;
    private final int patternCount;
    
    /**
     * 기본 생성자
     */
    public MailMessageFormatter() {
        String messageFormat = getMessageFormat();
        this.formatter = new MessageFormat(messageFormat);
        this.patternCount = getPatternCount(messageFormat);
    }
    
    private int getPatternCount(String messageFormatter) {
        Pattern pattern = Pattern.compile("(\\{\\s*\\d+(\\s|\\:|[0-9a-zA-Z])*\\})");
        Matcher matcher = pattern.matcher(messageFormatter);
        
        int max = 0;
        while (matcher.find()) {
            Matcher subMatcher = Pattern.compile("\\d+").matcher(matcher.group());
            if (subMatcher.find()) {
                max = Math.max(max, Integer.parseInt(subMatcher.group()));
            }
        }
        return max + 1;
    }
    
    /**
     * <p>메일 메시지 포맷을 설정하는 메소드.
     * <p>이 메소드에서 포맷을 설정한다.
     * <p>예시:
     * <pre>
     * {@code
     * @Override
     * protected String getMessageFormat()
     *     return "{0}, {1}!";
     * }
     * </pre>
     * <p>이와 같이 메소드를 오버라이딩한 후 format() 메소드를 호출할 경우,
     * <pre>
     * {@code
     * MailMessageForamtter formatter = getInstance();
     * formatter.format("Hello", "World"); // 출력: Hello, World!
     * }
     * </pre>
     * <p>이렇게 된다. format() 메소드를 통해 텍스트를 삽입할 위치는 {숫자} 형식으로 지정할 수 있다.
     * <p>{}로 둘러싸인 숫자는 format() 메소드의 인자 중 해당 위치에 삽입될 인자의 순서를 나타낸다. 0부터 시작한다.
     * <p>똑같은 값을 두 군데 이상 삽입하고 싶으면 똑같은 숫자를 여러 군데 입력하면 된다.
     * 
     * @return 메시지의 포맷
     */
    protected abstract String getMessageFormat();

    /**
     * MailMessageFormatter 구현체가 지원하는 메일의 타입. {@link MailType}을 참고한다.
     * 
     * @return 해당 구현체가 지원하는 메일의 타입
     */
    public abstract MailType getSupportedMailType();
    
    /**
     * Formatter 형식에 맞게 문자열을 포매팅한다.
     * 
     * @param values 포맷팅된 문자열에 삽입될 인자
     * @return 포맷팅된 문자열
     * @throws IllegalArgumentException values의 길이와 포맷에 지정된 argument의 개수가 맞지 않을 경우
     */
    public final String format(Object... values) throws IllegalArgumentException {
        if (patternCount != values.length) {
            throw new IllegalArgumentException(
                    "argument 개수가 맞지 않습니다. "
                    + "요구되는 argument 개수: " + this.patternCount
                    + ", 전달된 argument 개수: " + values.length
            );
        }
        return this.formatter.format(values);
    }
}