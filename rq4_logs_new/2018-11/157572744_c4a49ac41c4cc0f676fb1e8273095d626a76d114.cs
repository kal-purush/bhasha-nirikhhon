using System;
using System.Numerics;
using System.Security.Policy;
using DSAAlgorythm.ExtensionMethods;
using DSAAlgorythm.Model;
using DSAAlgorythm.Services;

namespace DSAAlgorythm
{
    public class DsaAlgorithm
    {
        private DsaSystemParameters _systemParameters;

        public DsaSystemParameters Parameters
        {
            get => _systemParameters;
            set => _systemParameters = value ?? throw new ArgumentNullException(nameof(DsaSystemParameters));
        }

        public Signature Sign(byte[] message, BigInteger privateKey)
        {
            BigInteger k = CryptoRandomNumberProvider.GenerateRandomBigInteger(1, Parameters.Q);

            // compute first part of signature
            BigInteger r = BigInteger.ModPow(Parameters.G, k, Parameters.P) % Parameters.Q;

            // assuming that p is prime -> k and p are coprime; we can use Euler's theorem to calculate inverse k
            // https://en.wikipedia.org/wiki/Modular_multiplicative_inverse -> Using Euler's theorem
            BigInteger kInverse = BigInteger.ModPow(k, Parameters.P - 2, Parameters.P);

            //compute second part of signature
            BigInteger hashedMessage = Parameters.HashFunction.GetHashedMessage(message).CreatePositiveBigInteger();
            BigInteger s = (kInverse * (hashedMessage + privateKey * r)) % Parameters.Q;

            return new Signature(r, s);
        }

        // Verification goes as follow:
        // 1. Calculate factors:
        //  w = s^(-1) mod q
        //  u1 = (H(m) * w) mod q, where H() is hash function
        //  u2 = (r * w) mod q
        //  v = ( ( g^(u1) * y^(u2) ) mod p ) mod q 
        // 2. if v equals r signature is verified, otherwise not 
        public bool Verify(byte[] message, Signature signature, BigInteger publicKey)
        {
            // calculate w factor
            BigInteger inverseS = BigInteger.ModPow(signature.S, Parameters.Q - 2, Parameters.Q);
            BigInteger w = inverseS % Parameters.Q;

            //calculate u1 factor
            BigInteger hashedMessage = Parameters.HashFunction.GetHashedMessage(message).CreatePositiveBigInteger();
            BigInteger u1 = (hashedMessage * w) % Parameters.Q;

            //calculate u2 factor
            BigInteger u2 = (signature.R*w) % Parameters.Q;
            
            // calculate v factor
            // (A * B) mod C = (A mod C * B mod C) mod C
            // (g^u1 * y^u2) % p ) % g
            BigInteger firstExp = BigInteger.ModPow(Parameters.G, u1, Parameters.P);
            BigInteger secondExp = BigInteger.ModPow(publicKey, u2, Parameters.P);
           
            BigInteger v = ((firstExp * secondExp) % Parameters.P) % Parameters.G;
            if (v == signature.R)
            {
                return true;
            }
            else
            {
                return false;
            }
        }


    }
}