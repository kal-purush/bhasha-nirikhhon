using VirtoCommerce.Pages.Core.Models;
using VirtoCommerce.SearchModule.Core.Model;

namespace VirtoCommerce.Pages.Core.Converters;

public interface IPageDocumentConverter
{
    PageDocument ToPageDocument(SearchDocument searchDocument);
    IndexDocument ToIndexDocument(PageDocument pageDocument);
}