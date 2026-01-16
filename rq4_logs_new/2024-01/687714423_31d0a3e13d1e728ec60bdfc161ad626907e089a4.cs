using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tlv.Search.Common;

namespace Tlv.Recall.Services
{
    public interface ISearchService
    {
        Task<List<SearchItem>> Search(string embeddingsProviderName,
                                      string prompt,
                                      ulong limit = 1);
        Task<List<SearchItem>> Search(string prompt,
                                      int limit = 1);
    }
}