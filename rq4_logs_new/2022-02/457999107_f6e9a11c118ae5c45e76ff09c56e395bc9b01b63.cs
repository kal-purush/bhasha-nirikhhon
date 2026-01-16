using System;

namespace Abstractions
{
    public interface ITimeModel
    {
        IObservable<int> GameTime { get; }
    }
}