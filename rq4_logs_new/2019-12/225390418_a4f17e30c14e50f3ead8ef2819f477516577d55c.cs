using System.Security.Cryptography.X509Certificates;

namespace Common
{
    public static class CertManager
    {
        /// <summary>
        /// Get a certificate with the specified subject name from the predefined certificate
        /// storage. Only valid certificates should be considered
        /// </summary>
        /// <returns>The requested certificate. If no valid certificate is found, returns null.</returns>
        public static X509Certificate2 GetCertificateFromStorage(StoreName storeName, StoreLocation storeLocation, string subjectName)
        {
            var store = new X509Store(storeName, storeLocation);
            store.Open(OpenFlags.ReadOnly);

            /// Check whether the subjectName of the certificate is exactly the same as the given "subjectName"
            foreach (var cert in store.Certificates.Find(X509FindType.FindBySubjectName, subjectName, validOnly: true))
            {
                if (cert.SubjectName.Name.Equals(string.Format("CN={0}", subjectName)))
                {
                    return cert;
                }
            }

            return null;
        }
    }
}