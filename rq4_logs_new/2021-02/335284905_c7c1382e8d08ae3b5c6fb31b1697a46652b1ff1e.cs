using System;
using System.Collections.Generic;
using System.Text;
using GartnerProductFeeder.DbServices;
using GartnerProductFeeder.ProductFeeder;
using NUnit.Framework;

namespace GartnerProductFeeder.Tests
{
  class DbConnect
  {

    [Test(Description = "Test to verify if connection is not initialized then system throw exception.")]
    public void ConnectWithNoOrWrongCredentialString()
    {
       IDbConnect dataClient= new DbConnectSql(null);
      Assert.Throws<Exception>(
        delegate { dataClient.Create(new List<Product>()); }, "Invalid connection string!!"
      );
      Assert.Throws<Exception>(
        delegate { dataClient.Create(null); }, "Invalid connection string!!"
      );
    }

  }
}