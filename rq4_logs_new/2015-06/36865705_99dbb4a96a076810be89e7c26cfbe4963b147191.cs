using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace IISLogTrafficGenerator.Convert
{
	using IISLogTrafficGenerator.Logic.Convert;

	public class Converter
    {
        public IEnumerable<LogRow> GetRequestEnumerator(string pathToLogFile)
        {
            if (pathToLogFile == null) throw new ArgumentNullException("pathToLogFile");

            var reader = new IISLogReader(pathToLogFile);

            return reader.GetRequests();
          
        }
    }
}