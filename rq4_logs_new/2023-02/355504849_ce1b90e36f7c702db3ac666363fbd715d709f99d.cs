using EncryptingAndDecrypting.Product.ConcreteProducts;
using EncryptingAndDecrypting.Product;
using System;
using System.Collections.Generic;
using System.Text;

namespace EncryptingAndDecrypting.Creator.ConcreteCreator
{
    /// <summary>  
    /// DESEncryptionFactory 'ConcreteCreator' class  
    /// </summary> 
    internal class DESEncryptionFactory : EncryptionFactory
    {
        public DESEncryptionFactory()
        {
        }

        public override EncryptionProduct GetEncryptionProduct()
        {
            return new DESEncryptionProduct();
        }
    }
}