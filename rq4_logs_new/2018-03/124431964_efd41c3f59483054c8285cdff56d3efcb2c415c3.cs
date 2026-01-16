using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using movies.Models;
using Newtonsoft.Json;

namespace movies.Helpers
{
    public class DataService
    {
       
        public async Task<IList<Result>> GetItemsAsync(int pageIndex, int pageSize)
        {
            await Task.Delay(2000);

            HttpClient client = new HttpClient();

            //Fazendo um GET no serviço

            string sURL = "https://api.themoviedb.org/3/movie/popular?api_key=1f54bd990f1cdfb230adb312546d765d&language=en-US&page="+ pageIndex +"";
            var uri = new Uri(string.Format(sURL));
            var responsepaci = await client.GetAsync(uri);


            //Classe para deserializar o retorno
            var resultMovies = new MovieResults();

            //verificando se o status code é OK
            if (responsepaci.IsSuccessStatusCode)
            {
                //pegando o retorno de lendo em async
                var content = responsepaci.Content.ReadAsStringAsync().Result;


                //deserializando o conteudo para a classe result
                resultMovies = JsonConvert.DeserializeObject<MovieResults>(content);

            }

            return resultMovies.results;
        }

        public async Task<MovieDetailsResult> GetDetailsAsync(int movieId)
        {
            await Task.Delay(2000);

            HttpClient client = new HttpClient();

            //Fazendo um GET no serviço

            string sURL = "https://api.themoviedb.org/3/movie/"+ movieId +"?api_key=1f54bd990f1cdfb230adb312546d765d&language=en-US";
            var uri = new Uri(string.Format(sURL));
            var responsepaci = await client.GetAsync(uri);


            //Classe para deserializar o retorno
            var resultMoviesDetails = new MovieDetailsResult();

            //verificando se o status code é OK
            if (responsepaci.IsSuccessStatusCode)
            {
                //pegando o retorno de lendo em async
                var content = responsepaci.Content.ReadAsStringAsync().Result;


                //deserializando o conteudo para a classe result
                resultMoviesDetails = JsonConvert.DeserializeObject<MovieDetailsResult>(content);

            }

            return resultMoviesDetails;
        }

        public async Task<IList<Result>> GetSearchAsync(string query, int pageIndex, int pageSize)
        {
            await Task.Delay(2000);

            HttpClient client = new HttpClient();

            //Fazendo um GET no serviço

            string sURL = "https://api.themoviedb.org/3/search/movie?api_key=1f54bd990f1cdfb230adb312546d765d&language=en-US&query="+ query +"&page=1&include_adult=false&page=" + pageIndex + "";
            var uri = new Uri(string.Format(sURL));
            var responsepaci = await client.GetAsync(uri);


            //Classe para deserializar o retorno
            var resultMovies = new MovieResults();

            //verificando se o status code é OK
            if (responsepaci.IsSuccessStatusCode)
            {
                //pegando o retorno de lendo em async
                var content = responsepaci.Content.ReadAsStringAsync().Result;


                //deserializando o conteudo para a classe result
                resultMovies = JsonConvert.DeserializeObject<MovieResults>(content);

            }

            return resultMovies.results;
        }
    }
}