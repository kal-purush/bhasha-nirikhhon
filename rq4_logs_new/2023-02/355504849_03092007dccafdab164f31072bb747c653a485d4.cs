using EncryptingAndDecrypting.Product;
using EncryptingAndDecrypting.Product.ConcreteProducts;
using System;
using System.Collections.Generic;
using System.Text;

namespace EncryptingAndDecrypting.Creator.ConcreteCreator
{
    /// <summary>  
    /// TripleDESEncryptionFactory 'ConcreteCreator' class  
    /// </summary>  
    class TripleDESEncryptionFactory : EncryptionFactory
    {
        public TripleDESEncryptionFactory()
        { }

        public override EncryptionProduct GetEncryptionProduct()
        {
            return new TripleDESEncryptionProduct();
        }
    }
}