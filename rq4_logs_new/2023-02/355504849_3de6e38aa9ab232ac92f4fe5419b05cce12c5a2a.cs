using EncryptingAndDecrypting.Enums;
using EncryptingAndDecrypting.Product;
using EncryptingAndDecrypting.Product.ConcreteProducts;
using System;
using System.Collections.Generic;
using System.Text;

namespace EncryptingAndDecrypting.Creator.ConcreteCreator
{
    /// <summary>  
    /// A 'ConcreteCreator' class  
    /// </summary>  
    class AESEncryptionFactory : EncryptionFactory
    {
        public AESEncryptionFactory()
        {
        }

        public override EncryptionProduct GetEncryptionProduct()
        {
            return new AESEncryptionProduct();
        }
    }
}