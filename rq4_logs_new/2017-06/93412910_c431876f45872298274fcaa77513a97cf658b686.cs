namespace TCObjectStorageClient.ObjectStorage
{
    internal static class ObjectStorageUrl
    {
        internal static string UrlForToken()
        {
            return "https://api-compute.cloud.toast.com/identity/v2.0/tokens";
        }

        internal static string UrlForObject(string account, string containerName, string objectName)
        {
            return "https://api-storage.cloud.toast.com/v1/{account}/{containerName}/{objectName}";
        }

        internal static string UrlForContainer(string account, string containerName)
        {
            return "https://api-storage.cloud.toast.com/v1/{account}/{containerName}";
        }
    }
}