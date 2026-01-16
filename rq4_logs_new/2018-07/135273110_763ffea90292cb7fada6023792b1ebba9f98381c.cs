using System;
using CAFU.Core.Domain.UseCase;

namespace CAFU.Flow.Domain.UseCase
{
    public interface IMultipleFlowUseCase : IUseCase
    {
        /// <summary>
        /// Load object
        /// </summary>
        /// <param name="key">key of flow</param>
        void Load();

        /// <summary>
        /// Start flow
        /// </summary>
        /// <param name="key">key of flow</param>
        void Start(string key);

        /// <summary>
        /// Stop flow
        /// </summary>
        /// <param name="key">key of flow</param>
        void Stop(string key);

        /// <summary>
        /// Resume flow
        /// </summary>
        /// <param name="key">key of flow</param>
        void Resume(string key);

        /// <summary>
        /// Pause flow
        /// </summary>
        /// <param name="key">key of flow</param>
        void Pause(string key);

        /// <summary>
        /// Get id flow by key
        /// </summary>
        /// <param name="key">key of flow</param>
        /// <returns></returns>
        IObservable<int> GetIdFlowAsObservable(string key);
    }
}