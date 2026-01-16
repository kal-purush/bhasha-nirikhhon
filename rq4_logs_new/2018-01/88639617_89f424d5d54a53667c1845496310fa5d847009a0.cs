using System.Collections.Generic;

namespace MySelfLog.Adapter
{
    public interface IDiaryCache
    {
        IDictionary<string, string> GetDiaries();
    }
}