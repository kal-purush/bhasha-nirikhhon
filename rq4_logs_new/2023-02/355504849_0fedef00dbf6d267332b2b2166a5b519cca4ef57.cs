using EncryptingAndDecrypting.Enums;
using EncryptingAndDecrypting.Product;
using EncryptingAndDecrypting.Product.ConcreteProducts;
using System;
using System.Collections.Generic;
using System.Text;

namespace EncryptingAndDecrypting.Creator.ConcreteCreator
{
    /// <summary>  
    /// RSAEncryptionFactory 'ConcreteCreator' class  
    /// </summary>  
    class RSAEncryptionFactory : EncryptionFactory
    {
        public RSAEncryptionFactory()
        {
        }

        public override EncryptionProduct GetEncryptionProduct()
        {
            return new RSAEncryptionProduct();
        }
    }
}