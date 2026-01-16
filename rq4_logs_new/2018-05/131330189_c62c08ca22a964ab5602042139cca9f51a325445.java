package br.ufop.decom.gaid.queryExpansion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;
import org.jsoup.helper.StringUtil;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

public class ContentExtractor {
    private List<String> OmitElements;
    private StandardAnalyzer analyzer;


    public ContentExtractor() {
//        analyzer= analyzer = new StandardAnalyzer(Version.LUCENE_40);
        analyzer= analyzer = new StandardAnalyzer();
        OmitElements= new ArrayList<String>();
        BufferedReader reader;
        try {
            //stream = analyzer.tokenStream(null, new InputStreamReader(new URL("https://easylist-downloads.adblockplus.org/easylist.txt").openStream()));
            reader= new BufferedReader(new InputStreamReader(new URL("https://easylist-downloads.adblockplus.org/easylist.txt").openStream()));
            String line="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith("##")){
                    //System.out.println(line);
                    OmitElements.add(line.substring(2));
                }

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public String extractFromDocument(String html) {
        return extractFromDocument(Jsoup.parse(html));
    }

    public String extractFromDocument(Document doc){
        doc.select("nav, .nav, .navbar, [class*=nav]").remove();
        doc.select("header, .header, .head, #header, #head").remove();
        doc.select("footer, .footer, #footer").remove();
        doc.select(".sidebar, #sidebar, [class^=sidebar], aside, [class^=aside]").remove();
        doc.select("[class*=social], [id*=social], [class*=shar], [id*=share]").remove();
        doc.select("[class*=breadcrumb], [id*=breadcrumb]").remove();
        doc.select("[href*=facebook], [href*=twitter], [href*=youtube], [href*=pintrest]").remove();
        doc.select("iframe, embed").remove();

        //Elements adsInID=doc.select("[id^=ad]").remove();
        for(String t:OmitElements){
            try{
                doc.select(t).remove();}
            catch(org.jsoup.select.Selector.SelectorParseException e){

            }
            catch(Exception e1){

            }
        }
        String text="";
        try{
            text=getPlainText(doc);

        }
        catch(Exception e){
            System.out.println("Exception at text extraction");
        }
        return(text);
    }
    public String getPlainText(Element element) {
        FormattingVisitor formatter = new FormattingVisitor();
        NodeTraversor traversor = new NodeTraversor(formatter);
        traversor.traverse(element); // walk the DOM, and call .head() and .tail() for each node

        return formatter.toString();
    }

    // the formatting rules, implemented in a breadth-first DOM traverse
    private class FormattingVisitor implements NodeVisitor {
        private static final int maxWidth = 80;
        private int width = 0;
        private StringBuilder accum = new StringBuilder(); // holds the accumulated text

        // hit when the node is first seen
        public void head(Node node, int depth) {
            String name = node.nodeName();
            if (node instanceof TextNode)
                append(((TextNode) node).text()); // TextNodes carry all user-readable text in the DOM.
            else if (name.equals("li"))
                append("\n * ");
            else if (name.equals("dt"))
                append("  ");
            else if (StringUtil.in(name, "p", "h1", "h2", "h3", "h4", "h5", "tr"))
                append("\n");
        }

        // hit when all of the node's children (if any) have been visited
        public void tail(Node node, int depth) {
            String name = node.nodeName();
            if (StringUtil.in(name, "br", "dd", "dt", "p", "h1", "h2", "h3", "h4", "h5"))
                append("\n");
        }

        // appends text to the string builder with a simple word wrap method
        private void append(String text) {
            if (text.startsWith("\n"))
                width = 0; // reset counter if starts with a newline. only from formats above, not in natural text
            if (text.equals(" ") &&
                    (accum.length() == 0 || StringUtil.in(accum.substring(accum.length() - 1), " ", "\n")))
                return; // don't accumulate long runs of empty spaces

            if (text.length() + width > maxWidth) { // won't fit, needs to wrap
                String words[] = text.split("\\s+");
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    boolean last = i == words.length - 1;
                    if (!last) // insert a space if not the last word
                        word = word + " ";
                    if (word.length() + width > maxWidth) { // wrap and reset counter
                        accum.append("\n").append(word);
                        width = word.length();
                    } else {
                        accum.append(word);
                        width += word.length();
                    }
                }
            } else { // fits as is, without need to wrap text
                accum.append(text);
                width += text.length();
            }
        }

        @Override
        public String toString() {
            return accum.toString();
        }
    }
}