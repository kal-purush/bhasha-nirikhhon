using MooPromise.Enumerable;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MooPromise
{
    public static class Enumerables
    {
        public static IPromise<IPromiseEnumerator<T>> Async<T>(this IEnumerable<IPromise<T>> items)
        {
            return PromiseEnumerator.Create(items);
        }

        public static IPromise<IPromiseEnumerator<T>> Async<T>(this IEnumerable<T> items)
        {
            return PromiseEnumerator.Create(items);
        }

        public static IPromise<IPromiseEnumerator<T>> Async<T, E>(this IPromise<E> items) where E : IEnumerable<T>
        {
            return PromiseEnumerator.Create(items.Cast<IEnumerable<T>>());
        }

        public static IPromise<IPromiseEnumerator<T>> Where<T>(this IPromise<IPromiseEnumerator<T>> items, Func<T, int, IPromise<bool>> action)
        {
            return items.Then(x => WhereEnumerator.Create(x, action));
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<IPromise<T>> items, Func<T, int, IPromise<bool>> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<T> items, Func<T, int, IPromise<bool>> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T, E>(this IPromise<E> items, Func<T, int, IPromise<bool>> action) where E : IEnumerable<T>
        {
            return items.Async<T, E>().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> Where<T>(this IPromise<IPromiseEnumerator<T>> items, Func<T, int, bool> action)
        {
            return items.Then(x => WhereEnumerator.Create(x, action));
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<IPromise<T>> items, Func<T, int, bool> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<T> items, Func<T, int, bool> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T, E>(this IPromise<E> items, Func<T, int, bool> action) where E : IEnumerable<T>
        {
            return items.Async<T, E>().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> Where<T>(this IPromise<IPromiseEnumerator<T>> items, Func<T, IPromise<bool>> action)
        {
            return items.Then(x => WhereEnumerator.Create(x, action));
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<IPromise<T>> items, Func<T, IPromise<bool>> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<T> items, Func<T, IPromise<bool>> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T, E>(this IPromise<E> items, Func<T, IPromise<bool>> action) where E : IEnumerable<T>
        {
            return items.Async<T, E>().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> Where<T>(this IPromise<IPromiseEnumerator<T>> items, Func<T, bool> action)
        {
            return items.Then(x => WhereEnumerator.Create(x, action));
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<IPromise<T>> items, Func<T, bool> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T>(this IEnumerable<T> items, Func<T, bool> action)
        {
            return items.Async().Where(action);
        }

        public static IPromise<IPromiseEnumerator<T>> WhereAsync<T, E>(this IPromise<E> items, Func<T, bool> action) where E : IEnumerable<T>
        {
            return items.Async<T, E>().Where(action);
        }
    }
}