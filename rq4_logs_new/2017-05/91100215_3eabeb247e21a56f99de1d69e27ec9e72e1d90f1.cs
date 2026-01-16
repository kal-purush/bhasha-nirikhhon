using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;

namespace ExcelRead
{
    class CSharpWriter
    {
        public static readonly CSharpWriter Instance = new CSharpWriter();

        public void WriteStyles(string filePath, ExcelReader xlReader)
        {
            if(!File.Exists(filePath))
            {
                Console.WriteLine("Unable to write to " + filePath + " file path does not exist.");
                return;
            }

            var contents = ReadFile(filePath);
            contents = DeleteStyles(contents);
            var index = GetStyleIndex(contents);
            var strReader = new StringReader(contents);
            var line = strReader.ReadLine();

            using (var file = new StreamWriter(filePath))
            {
                while (index > -1)
                {
                    file.WriteLine(line);
                    line = strReader.ReadLine();
                    --index;
                }

                for (int i = 2; i <= xlReader.Rows; ++i)
                {
                    file.WriteLine(CreateNewStyle(xlReader.GetLine(i)));
                }

                while (line != null)
                {
                    file.WriteLine(line);
                    line = strReader.ReadLine();
                }
            }
        }

        string CreateNewStyle(string[] line)
        {
            var styles = new StringBuilder();
            var styleCode = GetStyleCode(line[2]);
            var familyCode = GetFamilyCode(line[3]);
            var dValue = GetDecimalValue(line[6]);
            var gravMin = dValue == null ? "null" : dValue + "m";
            dValue = GetDecimalValue(line[7]);
            var gravMax = dValue == null ? "null" : dValue + "m";

            var tab3 = "\t\t\t";
            var tab4 = "\t\t\t\t";

            styles.AppendLine($"{tab3}context.Styles.Add(new Style");
            styles.AppendLine($"{tab3}{{");
            styles.AppendLine($"{tab4}Code = \"{styleCode}\",");
            styles.AppendLine($"{tab4}Name = \"{line[2]}\",");
            styles.AppendLine($"{tab4}OriginalGravityMinimum = {gravMin},");
            styles.AppendLine($"{tab4}OriginalGravityMaximum = {gravMax},");
            styles.AppendLine($"{tab4}Description = \"{line[16]}\",");
            styles.AppendLine($"{tab4}StyleTags = new List<StyleTagAssociation> {{ new StyleTagAssociation {{ StyleCode = \"{styleCode}\", StyleTagCode = \"{familyCode}\"}}}}");
            styles.AppendLine($"{tab3}}});");

            return styles.ToString();
        }

        string GetStyleCode(string value)
        {
            var str = value.ToLower();
            var pattern = @"[a-z]+";

            var rgx = new Regex(pattern);
            var matches = rgx.Matches(str);

            var result = new StringBuilder();
            foreach(var match in matches)
            {
                result.Append(match).Append("-");
            }
            result.Remove(result.Length - 1, 1);

            return $"bjcp-2015.{result.ToString()}"; 
        }

        string GetFamilyCode(string value)
        {
            var str = new StringBuilder(GetStyleCode(value));

            if(!str.ToString().Contains("specialty-beer"))
            {
                str.Append("-family");
            }

            return str.ToString();
        }

        decimal? GetDecimalValue(string value)
        {
            decimal d;

            if(decimal.TryParse(value, out d))
            {
                return d;
            }

            return null;
        }

        string ReadFile(string filePath)
        {
            var str = "";

            string line;

            using (var file = new System.IO.StreamReader(filePath))
            {
                while ((line = file.ReadLine()) != null)
                {
                    str += line + "\n";
                }
            }

            return str;
        }

        int GetStyleIndex(string contents)
        {
            var strReader = new StringReader(contents);
            var line = strReader.ReadLine();
            var index = 0;
            
            while (line != null)
            {
                line = RemoveWhitespace(line);

                if(line.Equals(@"//styles"))
                {
                    return index;
                }
                line = strReader.ReadLine();
                ++index;
            }

            return index;
        }

        string DeleteStyles(string contents)
        {
            var strReader = new StringReader(contents);
            var line = strReader.ReadLine();
            var str = "";

            while (line != null)
            {
                var nLine = RemoveWhitespace(line);

                if (nLine.Contains("context.Styles.Add"))
                {
                    while(!nLine.Equals("});"))
                    {
                        line = strReader.ReadLine();
                        nLine = RemoveWhitespace(line);
                    }
                    line = strReader.ReadLine();
                    line = "+";
                }
                
                str += !line.Equals("+") ? line + "\n" : "";
                line = strReader.ReadLine();
            }

            return str;
        }

        static string RemoveWhitespace(string input)
        {
            return new string(input.ToCharArray().Where(c => !Char.IsWhiteSpace(c)).ToArray());
        }
    }
}