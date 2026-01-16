using System.Configuration;
using System.Globalization;

namespace FileSystemListener.Configuration
{
    public class FileSystemListenerConfigSection : ConfigurationSection
    {
        [ConfigurationProperty("culture", DefaultValue = "en-US", IsRequired = false)]
        public CultureInfo Culture => (CultureInfo)this["culture"];

        [ConfigurationCollection(typeof(DirectoryElement), AddItemName = "directory")]
        [ConfigurationProperty("directories", IsRequired = false)]
        public DirectoryElementCollection Directories => (DirectoryElementCollection)this["directories"];

        [ConfigurationCollection(typeof(RuleElement), AddItemName = "rule")]
        [ConfigurationProperty("rules", IsRequired = true)]
        public RuleElementCollection Rules => (RuleElementCollection)this["rules"];
    }
}