using Newtonsoft.Json;
using PCLStorage;
using System.Net.Http;
using System.Threading.Tasks;

namespace MovieFetcher.Core
{
    class JSONHandler
    {
        public async Task<YIFYMovies> ParseJsonAsync(string url)
        {
            HttpClient htClient = new HttpClient();
            var jsonResponse = await htClient.GetStringAsync(url);
            // await WriteJSONToLocalFileAsync(jsonResponse);
            var YIFYMovies = JsonConvert.DeserializeObject<YIFYMovies>(jsonResponse);

            return YIFYMovies;
        }

        private async Task WriteJSONToLocalFileAsync(string JSONContent)
        {
            string fileName = "JSONMovieData.txt";
            IFolder rootFolder = FileSystem.Current.LocalStorage;
            IFolder folder = await rootFolder.CreateFolderAsync("storage", CreationCollisionOption.OpenIfExists);
            IFile file = await folder.CreateFileAsync(fileName, CreationCollisionOption.ReplaceExisting);
            await file.WriteAllTextAsync(JSONContent);
        }

        public async Task<string> ReadJSONFromFileAsync()
        {
            IFolder folder = FileSystem.Current.LocalStorage;
            IFile file = await folder.GetFileAsync("JSONMovieData.txt");
            var jsonContent = await file.ReadAllTextAsync();

            return jsonContent;
        }

    }
}