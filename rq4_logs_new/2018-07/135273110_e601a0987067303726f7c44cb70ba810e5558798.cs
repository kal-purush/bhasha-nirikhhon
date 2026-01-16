using System;
using CAFU.Core.Domain.UseCase;

namespace CAFU.Flow.Domain.UseCase
{
    public interface IFlowUseCase : IUseCase
    {
        /// <summary>
        /// Loads FlowModels from FlowEntityList
        /// </summary>
        void Load();

        /// <summary>
        /// Start flow
        /// </summary>
        void Start();

        /// <summary>
        /// Stop flow
        /// </summary>
        void Stop();

        /// <summary>
        /// Resume flow
        /// </summary>
        void Resume();

        /// <summary>
        /// Pause flow
        /// </summary>
        void Pause();

        /// <summary>
        /// Flow of Ids.
        /// </summary>
        IObservable<int> IdFlowAsObservable { get; }
    }
}