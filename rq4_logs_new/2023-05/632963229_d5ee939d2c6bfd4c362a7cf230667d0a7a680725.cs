using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MovieFiles.Core.Models
{
    public class MyMovieListItem
    {
        public enum ListType{
            WATCH_LATER
        }
        
        public Guid UserId {get;set;}
        public int MovieId {get;set;}
        public int ListId {get;set;}
        public string ListName {get;set;} = "";

        public static string GetListTypeName(ListType type){
            switch (type){
                case MyMovieListItem.ListType.WATCH_LATER: return "WATCH_LATER";
                default: throw new ArgumentOutOfRangeException("Undefined list type");
            }
        }

        public static MyMovieListItem CreateWatchLaterListItem(int movieId, Guid userId){
            return new MyMovieListItem(){
                UserId = userId,
                MovieId = movieId,
                ListName = GetListTypeName(ListType.WATCH_LATER)
            };
        }
    }
}