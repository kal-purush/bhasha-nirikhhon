using System.Text;
using Apps.Jira.Dtos;

namespace Apps.Jira.Utils;

public static class JiraDocToMarkdownConverter
{
    public static string ConvertToMarkdown(Description description)
    {
        var markdown = new StringBuilder();
        foreach (var element in description.Content)
            ProcessContentElement(element, markdown, 0);
        return markdown.ToString();
    }

    private static void ProcessContentElement(ContentElement element, StringBuilder markdown, int indentLevel)
    {
        switch (element.Type)
        {
            case "paragraph":
                foreach (var content in element.Content)
                    ProcessContentElement(content, markdown, indentLevel);
                markdown.AppendLine("\n");
                break;

            case "text":
                string text = element.Text;
                if (element.Marks != null && element.Marks.Any())
                {
                    var linkMark = element.Marks.FirstOrDefault(m => m.Type == "link");
                    if (linkMark != null)
                    {
                        var href = linkMark.Attrs?.Href ?? "#";
                        text = $"[{text}]({href})";
                    }
                    foreach (var mark in element.Marks.Where(m => m.Type != "link"))
                    {
                        switch (mark.Type)
                        {
                            case "strong":
                                text = $"**{text}**";
                                break;
                            case "em":
                                text = $"*{text}*";
                                break;
                        }
                    }
                }
                markdown.Append(text);
                break;

            case "bulletList":
                foreach (var listItem in element.Content)
                    ProcessContentElement(listItem, markdown, indentLevel);
                markdown.AppendLine();
                break;

            case "listItem":
                foreach (var content in element.Content)
                {
                    if (content.Type == "paragraph")
                    {
                        markdown.Append(new string(' ', indentLevel * 2) + "- ");
                        ProcessContentElement(content, markdown, indentLevel);
                        markdown.AppendLine();
                    }
                    else if (content.Type == "bulletList")
                    {
                        ProcessContentElement(content, markdown, indentLevel + 1);
                    }
                    else
                    {
                        ProcessContentElement(content, markdown, indentLevel);
                    }
                }
                break;

            default:
                if (element.Content != null)
                {
                    foreach (var content in element.Content)
                        ProcessContentElement(content, markdown, indentLevel);
                }
                break;
        }
    }
}