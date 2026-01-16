using ServerKeyLogsParser.CommonComponents.DataConverters.Basic;
using ServerKeyLogsParser.ParserComponents.MediumStores;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServerKeyLogsParser.ParserComponents.DataConverters
{
    class MappingIDWithNameWithHostConverter : DataConverter<DataSet, List<MappingIdWithNameWithHost>>
    {
        public List<MappingIdWithNameWithHost> convert(DataSet data)
        {
            List<MappingIdWithNameWithHost> result = new List<MappingIdWithNameWithHost>();
            for (int i = 0; i < data.Tables[0].Rows.Count; i++)
            {
                int id = int.Parse(data.Tables[0].Rows[i][0].ToString());
                string name = data.Tables[0].Rows[i][1].ToString();
                name = name.Replace(" ", "");
                string host = data.Tables[0].Rows[i][2].ToString();
                host = host.Replace(" ", "");

                result.Add(new MappingIdWithNameWithHost(id, name, host));
            }
            return result;
        }
    }
}