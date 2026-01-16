using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EtlPackage
{
    //public interface Executable
    //{

    //}
    public class EtlPackageInfo
    {
        string _packageName;
        ArrayList _dataFlowDestinationTables;
        public EtlPackageInfo(string packageName)
        {
            _packageName = packageName;
            _dataFlowDestinationTables = new ArrayList();
        }
        public void DataFlowDestinationTableCallBack(string fullyQualifiedTableName)
        {
            _dataFlowDestinationTables.Add(fullyQualifiedTableName);
        }
    }
}