using System;
using System.IO;
using System.Net.Mime;

namespace Ridavei.FileCryption
{
    public abstract class AFileCryptionBuilder
    {
        protected Func<object, Stream> FileLoadMethod;
        protected Func<Stream, ContentType, string, Stream> CryptionMethod;

        protected AFileCryptionBuilder() { }

        public AFileCryptionBuilder SetFileLoaderMethod(Func<object, Stream> func)
        {
            FileLoadMethod = func;
            return this;
        }

        public AFileCryptionBuilder SetEncryptionMethod(Func<Stream, ContentType, string, Stream> func)
        {
            CryptionMethod = func;
            return this;
        }

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