using System;

namespace Core.Serialization.JSON
{
	public class
                                    SettingsNewtonsoftJson
                                    :
                                    Settings
    {
		public
                                    SettingsNewtonsoftJson
                                    (
									)
		{
            return;
		}

        // Newtonsoft.Json.Serialization.DefaultContractResolver
        // https://www.newtonsoft.com/json/help/html/NamingStrategySkipSpecifiedNames.htm
        // https://www.newtonsoft.com/json/help/html/T_Newtonsoft_Json_Serialization_CamelCaseNamingStrategy.htm
        // https://stackoverflow.com/questions/48647650/newtonsoft-jsonconvert-serializeobject-ignoring-jsonproperty-if-name-is-uppercas
    }
}
