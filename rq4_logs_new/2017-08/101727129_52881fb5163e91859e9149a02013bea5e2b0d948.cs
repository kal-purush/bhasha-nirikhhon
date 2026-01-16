using System;
using System.Text;
using System.Windows;
using System.Xml;
using System.Diagnostics;
using System.Collections;
using System.Collections.Generic;
using System.IO;

using System.Windows.Media.Imaging;
using System.Windows.Documents;
using System.Threading;


namespace JustReader
{
    class ParseFbToXaml
    {
        XmlElement xRoot;
        XmlDocument xDoc;
        XmlNodeList nodeList;
        XmlNode node;
        XmlNode nodetemp;
        XmlNode nodeInfo;
        XmlAttribute attr;
        XmlNamespaceManager ns;

        XmlDocument xamlDoc;
        XmlElement xamlRoot;
        public string stringTitle = "Без названия";
        public string stringAuthor = "Автор неизвестен";
        public bool validFile = false;

        public int cntImage;
        public int cntBody;
        public int cntSection;
        public int cntToc;
        public Stack<string> notesStack = new Stack<string>();
        public Stack<string> imagesStack = new Stack<string>();
        public Stack<string> binaryStack = new Stack<string>();
        public Stack<string> tocStack = new Stack<string>();

        public ParseFbToXaml(string fn, int zip, string sxml)
        {
            // Decide what name to use as a root
            NameTable nt = new NameTable();
            ns = new XmlNamespaceManager(nt);
            ns.AddNamespace("fb", "http://www.gribuser.ru/xml/fictionbook/2.0");
            ns.AddNamespace("l", "http://www.w3.org/1999/xlink");
            ns.AddNamespace("NS1", "http://www.w3.org/1999/xlink");
            ns.AddNamespace("xlink", "http://www.w3.org/1999/xlink");

            cntImage = 1;
            cntBody = 1;
            cntSection = 1;
            notesStack.Clear();
            imagesStack.Clear();
            binaryStack.Clear();
            tocStack.Clear();

            xDoc = new XmlDocument();

            try
            {
                if (zip == 0)
                {
                    xDoc.Load(fn);
                }
                else
                {
                    xDoc.LoadXml(sxml);
                }
            }
            catch
            {
                MessageBox.Show("Не получается открыть fb файл. Возможно не валидный формат");
                return;
            }
            validFile = true;
            xRoot = xDoc.DocumentElement;
            nodeInfo = xRoot.SelectSingleNode("//fb:description/fb:title-info", ns);

            // Create an XmlDocument for generated xaml
            xamlDoc = new XmlDocument();
            xamlRoot = xamlDoc.CreateElement(null, "FlowDocument", _xamlNamespace);

            readTitle();
            readSequence();
            readAuthor();
            readCover();
            readAnnotation();
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            readBodies();

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds);
            Console.WriteLine("RunTime " + elapsedTime);

            xamlRoot.SetAttribute("xml:space", "preserve");
        }

        public string getXaml()
        {
            string xaml = xamlRoot.OuterXml;
            return xaml;
        }

        private void XmlTree(XmlElement xamlParentElement, XmlNode root, string tag, int level, string bodyname)
        {
            XmlElement xamlParent;
            string s;
            nodeList = root.ChildNodes;
            foreach (XmlNode nod in nodeList)
            {
//                if (nod.Name == "a") Console.WriteLine(nod.Name + " " + nod.NodeType);
                if (nod.NodeType.ToString() == "Element")
                {
                    switch (nod.Name)
                    {
                        case "v":
                        case "p":
                            xamlParent = AddParagraph(xamlParentElement, nod, tag, level, bodyname);
                            XmlTree(xamlParent, nod, tag, level + 1, bodyname);
                            break;
                        case "title":
                        case "cite":
                        case "epigraph":
                        case "annotation":
                        case "poem":
                        case "stanza":
                            xamlParent = AddSection(xamlParentElement, nod, nod.Name, level, bodyname);
                            XmlTree(xamlParent, nod, nod.Name, level+1, bodyname);
                            break;
                        case "a":
                            xamlParent = AddHyperlink(xamlParentElement, nod);
                            XmlTree(xamlParent, nod, nod.Name, level + 1, bodyname);
                            break;
                        case "emphasis":
                        case "strong":
                        case "sup":
                        case "strikethrough":
                        case "code":
                            xamlParent = AddInline(xamlParentElement, nod, nod.Name);
                            XmlTree(xamlParent, nod, tag, level + 1, bodyname);
                            break;                            
                        case "empty-line":
                        case "empty - line":
                            //AddBreak(xamlParentElement);
                            xamlParent = AddSection(xamlParentElement, null, "br", level, "");
                            break;
                        case "section":
                            xamlParent = AddSectionSection(xamlParentElement, nod, "section", level, bodyname);
                            XmlTree(xamlParent, nod, tag, level + 1, bodyname);
                            break;
                        case "image":
                            s = getImage(nod);
                            if (s != "")
                            {
                                if(xamlParentElement.Name == "Section")
                                {
                                    AddImageBin(xamlParentElement, "Paragraph", s);
                                }
                                else
                                {
                                    AddImageBin(xamlParentElement, "Image", s);
                                }
                            }
                            break;
                        case "table":
                            break;
                            /*                       

                                                    default:
                                                        xamlParent = AddParagraph(xamlParentElement, null, nod.Name);
                                                        XmlTree(xamlParent, nod, tag);
                                                        break;
                            */
                    }
                }
                else
                {
                    AddRun(xamlParentElement, nod);
                }
            }
        }
        public void readBodies()
        {
            XmlElement xamlParent;
            string s="";
            nodeList = xRoot.SelectNodes("//fb:body", ns);
            foreach (XmlNode nod in nodeList)
            {
                XmlAttribute attr = nod.Attributes["name"];
                if (attr == null) {
                    s = "";
                }
                else s = attr.Value.ToLower();
                
                if(s=="notes")
                {
                    AddHR(xamlRoot);
                }
                xamlParent = AddSection(xamlRoot, nod, "body", 0, s);
                XmlTree(xamlParent, nod, "", 0, s);
            }
        }
        public void readTitle()
        {
            node = nodeInfo.SelectSingleNode("fb:book-title", ns);

            if (node != null)
            {
                stringTitle = node.InnerText;
                XmlElement xamlParent = AddSection(xamlRoot, node, "head", 0, "");
                XmlTree(xamlParent, node, "head", 0, "");
            }
        }
        public void readSequence()
        {
            string s = "";
            node = nodeInfo.SelectSingleNode("fb:sequence", ns);
            if (node != null)
            {
                attr = node.Attributes["name"];
                if (attr != null) s = attr.Value;
                attr = node.Attributes["number"];
                if (attr != null) s += " № " + attr.Value;
            }
            s += " " + getDate();
            if (s != "")
            {
                XmlElement xamlParent = AddParagraph(xamlRoot, null, "sequence", 0, "");
                AddRunText(xamlParent, s);
            }
        }
        public string getDate()
        {
            string s = "";
            node = nodeInfo.SelectSingleNode("fb:date", ns);
            if (node != null)
            {
                s = node.InnerText;
            }
            return s;
        }
        public void readAuthor()
        {
            string s = "";
            nodeList = nodeInfo.SelectNodes("fb:author", ns);
            foreach (XmlNode nod in nodeList)
            {
                if (s != "") s += ", ";
                nodetemp = nod.SelectSingleNode("fb:first-name", ns);
                if (nodetemp != null)
                {
                    s += nodetemp.InnerText + " ";
                }
                nodetemp = nod.SelectSingleNode("fb:last-name", ns);
                if (nodetemp != null)
                {
                    s += nodetemp.InnerText;
                }
            }
            if (s != "")
            {
                stringTitle += " " + s;
                XmlElement xamlParent = AddParagraph(xamlRoot, null, "author", 0, "");
                AddRunText(xamlParent, s);
            }
        }
        public void readAnnotation()
        {
            node = nodeInfo.SelectSingleNode("fb:annotation", ns);
            if (node != null)
            {
                XmlElement xamlParent = AddSection(xamlRoot, node, "", 0, "");
                XmlTree(xamlParent, node, "annotation", 0, "");
            }
        }
        public void readCover()
        {
            node = nodeInfo.SelectSingleNode("fb:coverpage/fb:image", ns);
            string s = getImage(node);
            if(s!="")
            {
                AddImageBin(xamlRoot, "Paragraph", s);
            }
        }
        public string getImage(XmlNode node)
        {
            string s = "";
            if (node != null)
            {
                foreach (XmlAttribute at in node.Attributes)
                {
                    if (at.Name.IndexOf("href") > 0)
                    {
                        s = at.Value;
                        break;
                    }
                }
                if (s != "")
                {
                    s = s.Replace("#", "");
                    nodetemp = xRoot.SelectSingleNode("//fb:binary[@id='" + s + "']", ns);
                    if (nodetemp != null)
                    {
                        s = nodetemp.InnerText;
                    }
                }
            }
            return s;
        }

        //---------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------
        private XmlElement AddSectionSection(XmlElement xamlParentElement, XmlNode node, string tag, int level, string bodyname)
        {
            XmlElement xamlElement = xamlParentElement.OwnerDocument.CreateElement(/*prefix:*/null, "Section", _xamlNamespace);
            xamlParentElement.AppendChild(xamlElement);
            foreach (XmlAttribute at in node.Attributes)
            {
                if (at.Name.IndexOf("id") > -1)
                {
                    string s = "section_notes_"+at.Value;
                    xamlElement.SetAttribute("Name", s);                    
                    break;
                }
            }
            if (bodyname == "notes")
            {
                xamlElement.SetAttribute("Margin", "0, 15, 0, 5");
            }
            return xamlElement;
        }

        private XmlElement AddSection(XmlElement xamlParentElement, XmlNode node, string tag, int level, string bodyname)
        {
            string s = "";
            XmlElement xamlElement = xamlParentElement.OwnerDocument.CreateElement(/*prefix:*/null, "Section", _xamlNamespace);
            xamlParentElement.AppendChild(xamlElement);
            switch (tag)
            {
                case "head":
                    xamlElement.SetAttribute("FontSize", "48");
                    xamlElement.SetAttribute("TextAlignment", "Center");
                    break;
                case "body":
                    xamlElement.SetAttribute("Name", "body" + cntBody);
                    cntBody++;
                    break;
                case "title":
                    if (bodyname != "notes")
                    {
                        xamlElement.SetAttribute("FontSize", "32");
                        xamlElement.SetAttribute("TextAlignment", "Center");
                        xamlElement.SetAttribute("Margin", "0, 60, 0, 20");
                        s = "toc_" + cntToc;
                        xamlElement.SetAttribute("Name", s);
                        xamlElement.SetAttribute("Tag", level.ToString()+" "+ bodyname);
                        tocStack.Push(s);
                        cntToc++;
                    }
                    else
                    {
                        xamlElement.SetAttribute("FontSize", "24");
                        xamlElement.SetAttribute("Margin", "0, 0, 0, 0");
                        xamlElement.SetAttribute("Padding", "0, 3, 0, 0");
                    }
                    break;
                case "subtitle":
                    xamlElement.SetAttribute("FontSize", "27");
                    xamlElement.SetAttribute("TextAlignment", "Center");
                    break;
                case "poem":
                case "stanza":
                case "cite":
                case "epigraph":
                case "annotation":
                    break;  
            }
            return xamlElement;
        }

        private void AddImageBin(XmlElement xamlParentElement, string xamlName, string bin)
        {
            string s;
            XmlElement xamlElement;
            if (xamlName == "Paragraph")
            {
                xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Paragraph", _xamlNamespace);
                xamlElement.SetAttribute("TextAlignment", "Center");
                xamlParentElement.AppendChild(xamlElement);
                xamlParentElement = xamlElement;
                xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Image", _xamlNamespace);
            }
            else
            {
                xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Image", _xamlNamespace);
                xamlElement.SetAttribute("Margin", "0, 0, 10, 0");
                xamlElement.SetAttribute("VerticalAlignment", "Bottom");
                xamlElement.SetAttribute("HorizontalAlignment", "Left");
            }
            s = "img_bin_" + cntImage;
            imagesStack.Push(s);
            binaryStack.Push(bin);
            xamlElement.SetAttribute("Name", s);
            xamlElement.InnerText = " ";
            cntImage++;
            xamlParentElement.AppendChild(xamlElement);
        }


        private void AddHR(XmlElement xamlParentElement)
        {
            string s;
            XmlElement xamlElement;
            xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Paragraph", _xamlNamespace);
            xamlParentElement.AppendChild(xamlElement);
            xamlElement.SetAttribute("Padding", "0,20,0,20");
            xamlElement.SetAttribute("FontSize", "12");
            xamlElement.InnerText = "----------------------------------------------------------------------------------------";
        }


        private XmlElement AddParagraph(XmlElement xamlParentElement, XmlNode node, string tag, int level, string bodyname)
        {
            string s;
            XmlElement xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Paragraph", _xamlNamespace);
            xamlParentElement.AppendChild(xamlElement);
            if (bodyname == "notes")
            {
                xamlElement.SetAttribute("FontSize", "12");
                xamlElement.SetAttribute("Padding", "0,0,0,0");
                xamlElement.SetAttribute("Margin", "0,0,0,0");
            }

            switch (tag)
            {
                case "annotation":
                    xamlElement.SetAttribute("FontSize", "21");
                    xamlElement.SetAttribute("FontStyle", "Italic");
                    break;
                case "sequence":
                    xamlElement.SetAttribute("FontSize", "24");
                    xamlElement.SetAttribute("TextAlignment", "Center");
                    break;
                case "author":
                    xamlElement.SetAttribute("FontSize", "32");
                    xamlElement.SetAttribute("TextAlignment", "Center");
                    break;
                case "title":
                    if (bodyname != "notes")
                    {
                        xamlElement.SetAttribute("FontSize", "32");
                        xamlElement.SetAttribute("TextAlignment", "Center");
                    }
                    else
                    {
                        if(level==1) xamlElement.SetAttribute("FontSize", "28");
                        xamlElement.SetAttribute("TextAlignment", "Left");
                    }
                    break;
                case "subtitle":
                    xamlElement.SetAttribute("FontSize", "12");
                    xamlElement.SetAttribute("TextAlignment", "Center");
                    break;
                case "v":
                case "epigraph":
                case "poem":
                case "stanza":
                    xamlElement.SetAttribute("FontSize", "17");
                    xamlElement.SetAttribute("FontStyle", "Italic");
                    xamlElement.SetAttribute("Margin", "0,5,0,0");
                    xamlElement.SetAttribute("Padding", "0,0,0,0");
                    break;
                case "cite":
                    xamlElement.SetAttribute("FontSize", "21");
                    xamlElement.SetAttribute("FontStyle", "Italic");
                    break;
            }

            xamlElement.InnerText = " ";
            return xamlElement;
        }
        private XmlElement AddInline(XmlElement xamlParentElement, XmlNode node, string tag)
        {
            XmlElement xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Span", _xamlNamespace);
            switch (tag)
            {
                case "emphasis":
                    xamlElement.SetAttribute("FontStyle", "Italic");
                    break;
                case "strong":
                    xamlElement.SetAttribute("FontWeight", "Bold");
                    break;
                case "sub":
                    xamlElement.SetAttribute("FontSize", "12");
                    xamlElement.SetAttribute("BaselineAlignment", "Subscript");
                    break;
                case "sup":
                    xamlElement.SetAttribute("FontSize", "12");
                    xamlElement.SetAttribute("BaselineAlignment", "Superscript");
                    break;
                case "strikethrough":
                    xamlElement.SetAttribute("TextDecorations", "Strikethrough");
                    break;
                case "code":
                    xamlElement.SetAttribute("FontFamily", "Courier New");
                    break;
            }

            xamlParentElement.AppendChild(xamlElement);
            return xamlElement;
        }

        private void AddRun(XmlElement xamlParentElement, XmlNode node)
        {
            if (xamlParentElement.Name == "Section") {

                XmlElement xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Paragraph", _xamlNamespace);
                xamlParentElement.AppendChild(xamlElement);
                xamlElement.AppendChild(xamlParentElement.OwnerDocument.CreateTextNode(node.InnerText));
            }
            else xamlParentElement.AppendChild(xamlParentElement.OwnerDocument.CreateTextNode(node.InnerText));
        }
        private void AddRunText(XmlElement xamlParentElement, string txt)
        {
            xamlParentElement.InnerText = txt;
        }

        private static void AddBreak(XmlElement xamlParentElement)
        {
            XmlElement xamlLineBreak = xamlParentElement.OwnerDocument.CreateElement(null, "LineBreak", _xamlNamespace);
            xamlParentElement.AppendChild(xamlLineBreak);
        }
        private XmlElement AddHyperlink(XmlElement xamlParentElement, XmlNode htmlElement)
        {
            // Convert href attribute into NavigateUri and TargetName
            string s = "";
            string href = null;
            XmlElement xamlElement;
            foreach (XmlAttribute at in htmlElement.Attributes)
            {
                if (at.Name.IndexOf("href") > 0)
                {
                    href = at.Value;
                    break;
                }
            }

            if (href == null)
            {
                // When href attribute is missing - ignore the hyperlink
                xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Span", _xamlNamespace);
            }
            else
            {
                xamlElement = xamlParentElement.OwnerDocument.CreateElement(null, "Hyperlink", _xamlNamespace);

                string[] hrefParts = href.Split(new char[] { '#' });
                if (hrefParts.Length > 0 && hrefParts[0].Trim().Length > 0)
                {
                    xamlElement.SetAttribute("NavigateUri", hrefParts[0].Trim());
                }
                if (hrefParts.Length == 2 && hrefParts[1].Trim().Length > 0)
                {
                    s = hrefParts[1].Trim();
                    xamlElement.SetAttribute("BaselineAlignment", "Superscript");
                    xamlElement.SetAttribute("FontSize", "14");
                    xamlElement.SetAttribute("TargetName", s);
                    s = "notes_" + s;
                    xamlElement.SetAttribute("Name", s);
                    notesStack.Push(s);
                    //AddRunText(xamlParentElement, " ");
                }

                // Add the new element to the parent.
                xamlParentElement.AppendChild(xamlElement);
            }
            return xamlElement;
        }


               

        #region Private Fields

        static string _xamlNamespace = "http://schemas.microsoft.com/winfx/2006/xaml/presentation";
        #endregion Private Fields

    }
}