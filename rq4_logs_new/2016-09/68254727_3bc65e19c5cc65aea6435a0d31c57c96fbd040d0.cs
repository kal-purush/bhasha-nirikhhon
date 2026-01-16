using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SavingVariables.DAL;


namespace SavingVariables.Tests.DAL
{
    [TestClass]
    public class VariablesRepositoryTests
    {

        [TestMethod]
        public void EnsureCanCreateRepoInstance()
        {
            VariablesRepository repo = new VariablesRepository();
            Assert.IsNotNull(repo);
        }

        [TestMethod]
        public void EnsureRepoHasContext()
        {
            VariablesRepository repo = new VariablesRepository();

            VariablesContext actual_context = repo.Context;

            Assert.IsInstanceOfType(actual_context, typeof(VariablesContext));
        }

    }
}