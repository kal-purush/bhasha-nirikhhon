using CopelandTech.DependencyInjection.Interfaces;
using System;

namespace RandomCompanyName.TestServices
{
    public interface IRandomWorkerService : ISingletonService
    {

    }

    public class RandomWorkerService : IRandomWorkerService
    {
    }
}