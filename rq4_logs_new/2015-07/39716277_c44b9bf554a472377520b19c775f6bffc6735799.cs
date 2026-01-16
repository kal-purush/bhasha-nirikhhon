using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml;

namespace WG_ConfigDifficulty
{
    class XMLHelper
    {
        public static double takeParam(XmlNode node, string attributeName, double defaultValue)
        {
            try
            {
                return Double.Parse(node.Attributes[attributeName].InnerText, CultureInfo.CurrentCulture);
            }
            catch
            {
                return defaultValue;
            }
        }
    }
}