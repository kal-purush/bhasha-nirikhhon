package cnsa.demo.service.util;

public class EscapeSequenceConverter {
    public static String makeConvert(String content) {
        String convertContent = content;
        convertContent = convertContent.replaceAll("&", "&amp;");
        convertContent = convertContent.replaceAll(" ", "&nbsp;");
        convertContent = convertContent.replaceAll("<", "&lt;");
        convertContent = convertContent.replaceAll(">", "&gt;");
        convertContent = convertContent.replaceAll("\n", "<br>");
        convertContent = convertContent.replaceAll("\"", "&quot;");

        return convertContent;
    }
}