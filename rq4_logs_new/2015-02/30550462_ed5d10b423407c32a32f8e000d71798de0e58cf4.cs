using CqrsDal.Contracts;
using CqrsDomain.Mongo.Model;

namespace CqrsDal.Tests
{
    public class FakePushProductMessages : IPushProductMessages
    {
        public void ResetProductTable()
        {
        }

        public void PushUpdatedProduct(QueryProduct product)
        {           
        }

        public void PushUpdateMessage(string message)
        {            
        }

        public void PushUpdateMessage(int productId, string categoryName, QueryProduct queryProduct)
        {            
        }
    }
}