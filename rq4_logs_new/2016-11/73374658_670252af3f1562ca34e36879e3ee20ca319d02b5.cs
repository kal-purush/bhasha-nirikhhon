using Akavache;
using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Windows.Security.Cryptography;
using Windows.Security.Cryptography.Core;

namespace FootballClient.DataAccess
{
    static class Utility
    {
        public static class Md5Calculator
        {
            public static string ComputeMd5(string strValue)
            {
                var algorithm = HashAlgorithmProvider.OpenAlgorithm(HashAlgorithmNames.Md5);
                var buffer = CryptographicBuffer.ConvertStringToBinary(strValue, BinaryStringEncoding.Utf8);
                var hashed = algorithm.HashData(buffer);
                return CryptographicBuffer.EncodeToHexString(hashed);
            }
        }
    }

    public static class AkavacheExtensions
    {
        public static IObservable<T> GetAndUpdateObject<T> (this IBlobCache This, string key, Func<IObservable<T>> fetchFunc, DateTimeOffset? absoluteExpiration = null)
        {
            var result = fetchFunc().
                Do(v => This.InsertObject(key, v, absoluteExpiration));

            return result;
        }

        //public static IObservable<T> FetchOrGetObject<T>(this IBlobCache This, string key, Func<IObservable<T>> fetchFunc, DateTimeOffset? absoluteExpiration = null)
        //{
        //    var result = fetchFunc().
        //        Do(v => This.InsertObject(key, v, absoluteExpiration))
        //        .Catch<T, Exception>(ex =>
        //        {
        //            return This.GetObject<T>(key).Catch(Observable.Return(default(T)));
        //        });
        //
        //    
        //    return result;
        //}
    }
}