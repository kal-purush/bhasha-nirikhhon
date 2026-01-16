using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models;
using MovieFiles.Api.Client.Mappers;

namespace MovieFiles.Api.Client.Services
{
    public class MovieListService : BaseService, IMoiveListService
    {
        private IMovieDetailsService _movieDetailService;
        public MovieListService(string httpUrl, string functionAppKey, IMovieDetailsService movieDetailService) : base(httpUrl, functionAppKey)
        {
            _movieDetailService = movieDetailService;
        }

        public Task<bool> AddMovieToMovieList(Guid userId, int movieId, Core.Models.MyMovieListItem.ListType movieType)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveMovieFromMovieList(Guid userId, int movieId, Core.Models.MyMovieListItem.ListType movieType)
        {
            throw new NotImplementedException();
        }

        public async Task<CustomMovieList<Core.Models.MyMovieListItem>> GetMyMovieList(Guid userId, Core.Models.MyMovieListItem.ListType movieType, int page)
        {
            CustomMovieList_myMovieListItem response;
            switch (movieType) {
                case Core.Models.MyMovieListItem.ListType.WATCH_LATER:
                    response = await RetryHelper.RetryOnExceptionAsync<CustomMovieList_myMovieListItem>(3, () => _client.GetMoviesToWatchLaterAsync(userId,page,_functionAppKey));
                    break;
                default:
                    throw new ArgumentException("unknown type of list");
            }
            return ClientToUi.Map(response);
        }

        public async Task<Core.Models.MovieList> ConvertCustomMovieListToMovieList(CustomMovieList<Core.Models.MyMovieListItem> externList){
            Core.Models.MovieList myList = new(){
                Page = externList.Page,
                TotalResults = externList.TotalResults,
                TotalPages = externList.TotalPages
            };
            var getMoviesTasks = externList.List.Select(getMovieFromListItem).ToArray();
            myList.Results = await Task.WhenAll(getMoviesTasks);
            return myList;
        }

        private async Task<Core.Models.Movie> getMovieFromListItem(Core.Models.MyMovieListItem item){
            return await _movieDetailService.GetMovieDetailsAsync(item.MovieId);
        }
    }
}