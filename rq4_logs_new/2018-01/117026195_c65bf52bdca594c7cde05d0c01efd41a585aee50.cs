using System;
using System.Collections.Generic;
using System.IO;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AeroDataGenerator
{
    public class BlobUtils
    {
        public static Stream GetBlobStream(String connectionString, String container, String item)
        {
            Stream returnValue = null;

            try
            {
                CloudBlockBlob blob = GetBlockBlob(connectionString, container, item);
                if (blob != null)
                {
                    if (blob.Exists())
                    {
                        returnValue = blob.OpenRead();
                    }

                }
            }
            catch (Exception)
            {
            }

            return returnValue;
        }

        public static String GetContents(String connectionString, String container, String item)
        {
            String returnValue = String.Empty;

            try
            {
                CloudBlockBlob blob = GetBlockBlob(connectionString, container, item);
                if (blob != null)
                {
                    if (blob.Exists())
                    {
                        returnValue = blob.DownloadText();
                    }

                }
            }
            catch (Exception)
            {
            }

            return returnValue;
        }

        private static CloudBlockBlob GetBlockBlob(String connectionString, String container, String blobName)
        {
            CloudBlockBlob returnValue = null;

            if (!String.IsNullOrEmpty(container) && !String.IsNullOrEmpty(connectionString) &&
                !String.IsNullOrEmpty(blobName))
            {
                CloudBlobClient client = GetBlobClient(connectionString);
                if (client != null)
                {
                    if (CreateContainer(client, container))
                    {
                        CloudBlobContainer itemContainer = client.GetContainerReference(container);

                        if (itemContainer.Exists())
                        {
                            returnValue = itemContainer.GetBlockBlobReference(blobName);
                        }
                    }
                }
            }

            return returnValue;
        }

        private static bool CreateContainer(CloudBlobClient client, string containerName)
        {
            bool returnValue = false;

            if (!String.IsNullOrEmpty(containerName) && client != null)
            {
                try
                {
                    String directoryName = String.Empty;

                    if (containerName.Contains("/"))
                    {
                        int idx = containerName.IndexOf('/');
                        directoryName = containerName.Substring(idx + 1);
                        containerName = containerName.Substring(0, idx);
                    }

                    CloudBlobContainer itemContainer = client.GetContainerReference(containerName.ToLower());

                    itemContainer.CreateIfNotExists();
                    itemContainer.SetPermissions(
                        new BlobContainerPermissions
                        {
                            PublicAccess =
                                BlobContainerPublicAccessType.Blob
                        });

                    if (!String.IsNullOrEmpty(directoryName))
                    {
                        CloudBlobDirectory directory = itemContainer.GetDirectoryReference(directoryName);
                    }
                }
                catch
                {
                    // No way to log this as of yet....
                }

                returnValue = true;
            }

            return returnValue;
        }

        private static CloudBlobClient GetBlobClient(String connectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            return storageAccount.CreateCloudBlobClient();
        }

    }
}