using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBus.Configuration
{
    [ConfigurationCollection(typeof(BusSettingsElement))]
    public class BusSettings: ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new BusSettingsElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((BusSettingsElement) element).Key;
        }

        public BusSettingsElement this[int index] => (BusSettingsElement)BaseGet(index);

        public new BusSettingsElement this[string key] => (BusSettingsElement)BaseGet(key);
    }

    public class BusSettingsElement: ConfigurationElement
    {
        [ConfigurationProperty("key", IsKey = true, IsRequired = true)]
        public string Key
        {
            get => (string)base["key"];
            set => base["key"] = value;
        }

        [ConfigurationProperty("value", IsRequired = true)]
        public string Value
        {
            get => (string) base["value"];
            set => base["value"] = value;
        }
    }
}