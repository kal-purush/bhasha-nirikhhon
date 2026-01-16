using System;
using System.IO;
using System.Net.Mime;

namespace Ridavei.FileCryption
{
    /// <summary>
    /// Base class used for file encryption/decryption builders.
    /// </summary>
    public abstract class AFileCryptionBuilderBase
    {
        /// <summary>
        /// Method used for loading file.
        /// </summary>
        protected Func<object, Stream> FileLoadMethod;

        /// <summary>
        /// Method used for encryption/decryption.
        /// </summary>
        protected Func<Stream, ContentType, string, Stream> CryptionMethod;

        /// <summary>
        /// Hided constructor for <see cref="AFileCryptionBuilderBase"/>.
        /// </summary>
        protected AFileCryptionBuilderBase() { }

        /// <summary>
        /// Sets the method used for loading file.
        /// </summary>
        /// <param name="func"></param>
        /// <returns>Builder</returns>
        public AFileCryptionBuilderBase SetFileLoaderMethod(Func<object, Stream> func)
        {
            FileLoadMethod = func;
            return this;
        }

        /// <summary>
        /// Set the method used for encryption/decryption.
        /// </summary>
        /// <param name="func"></param>
        /// <returns>Builder</returns>
        public AFileCryptionBuilderBase SetEncryptionMethod(Func<Stream, ContentType, string, Stream> func)
        {
            CryptionMethod = func;
            return this;
        }

        /// <summary>
        /// File encryption/decryption method that returns <see cref="Stream"/>.
        /// </summary>
        /// <param name="fileInfoForLoaderMethod"><see cref="Object"/> that contains basic information used by the loader method.</param>
        /// <param name="contentType">Represents the MIME Content-Type header.</param>
        /// <param name="password">Password used for encryption/decryption.</param>
        /// <returns><see cref="Stream"/> of encrypted/decrypted file.</returns>
        /// <exception cref="ArgumentNullException">Exception throwed when none of the methods are set or file info are null.</exception>
        /// <exception cref="FileNotFoundException">Exception throwed when the file was not found.</exception>
        protected virtual Stream Cryption(object fileInfoForLoaderMethod, ContentType contentType, string password)
        {
            if (FileLoadMethod == null)
                throw new ArgumentNullException(nameof(FileLoadMethod));
            if (CryptionMethod == null)
                throw new ArgumentNullException(nameof(CryptionMethod));
            if (fileInfoForLoaderMethod == null)
                throw new ArgumentNullException(nameof(fileInfoForLoaderMethod));

            using (Stream file = FileLoadMethod(fileInfoForLoaderMethod))
            {
                if (file == null)
                    throw new FileNotFoundException("File doesn't exists");

                return CryptionMethod(file, contentType, password);
            }
        }
    }
}