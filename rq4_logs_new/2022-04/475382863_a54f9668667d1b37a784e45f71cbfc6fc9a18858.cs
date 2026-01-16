using System;
using System.Threading.Tasks;

namespace MVP_CleanArchitecture_Example.Scripts.Domain
{
    public interface IEndpointHelper
    {
        Task PurchaseBundle(int bundleId, Action onSucceed, Action onFailed);
    }

    public class EndpointHelper : IEndpointHelper
    {
        public async Task PurchaseBundle(int bundleId, Action onSucceed, Action onFailed)
        {
            var delayTime = TimeSpan.FromSeconds(UnityEngine.Random.Range(.5f, 1.5f));
            await Task.Delay(delayTime);
            
            onSucceed?.Invoke();
        }
    }
}