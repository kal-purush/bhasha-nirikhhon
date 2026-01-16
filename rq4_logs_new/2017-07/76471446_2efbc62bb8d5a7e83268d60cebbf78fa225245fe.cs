using System.Threading.Tasks;

namespace SFA.DAS.ReferenceData.Domain.Interfaces.Caching
{
    public interface ICachedRepository
    {
        Task RefreshCache();
    }
}