using Spring.Core.CDH.Autowire;
using Test.Service.MyTable;

namespace Test.Controllers
{
    public class TransactionController
    {
        [Autowire]
        public ITransactionMyTableService TransactionMyTableService { get; set; }
    }
}