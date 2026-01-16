using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ClinicOrganization.Tests
{
    public class JanitorTests
    {
        [Fact]
        public void Janitor_Info()
        {
            Janitor janitor = new Janitor(true);
            //Assert.Equal(40000, janitor.Salary);
            //Assert.Equal(501, janitor.Num);
            //Assert.Equal("Bob", janitor.Name);
            Assert.True(janitor.IsSweeping);
        }
    }
}