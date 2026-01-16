using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace HexMaster.Email.DomainModels
{
    public class Body
    {
        public string Name { get; private set; }
        public string Content { get; private set; }
        [JsonIgnore]
        public bool IsHtml { get; private set; }
        public bool IsDefault { get; private set; }

        public void SetName(string value)
        {
            Name = value;
        }
        public void SetContent(string value)
        {
            Content = value;
            IsHtml = ContentIsHtml();
        }
        internal void SetDefault(bool value)
        {
            IsDefault = value;
        }

        private bool ContentIsHtml()
        {
            return Regex.IsMatch(Content, RegularExpression.Html,
                RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        }

        public Body(string name, string content, bool isDefault = false)
        {
            Name = name;
            Content = content;
            IsDefault = isDefault;
            IsHtml = ContentIsHtml();
        }
    }
}