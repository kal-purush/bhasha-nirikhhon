using System;

public class Maybe<TValue>
{
    public readonly TValue Value;
    public readonly bool HasValue;

    internal Maybe(TValue value, bool hasValue)
    {
        this.Value = value;
        this.HasValue = hasValue;
    }

    /// <summary>
    /// LINQ query expression pattern.
    /// Takes a function which transforms the value contained by Maybe to another type.  
    /// </summary>
    /// <returns>
    /// An instance of Maybe containing an instance of the result type.
    /// </returns>
    public Maybe<TResult> Select<TResult>(Func<TValue, TResult> mapperExpression) =>
        this.HasValue
        ? MaybeFactory.Some(mapperExpression(this.Value))
        : MaybeFactory.None<TResult>();

    /// <summary>
    /// LINQ query expression pattern.
    /// Analog to "bind" or Scala's "flatMap".
    /// </summary>
    public Maybe<TResult> SelectMany<TIntermediate, TResult>(
        Func<TValue, Maybe<TIntermediate>> mapper,
        Func<TValue, TIntermediate, TResult> getResult)
    {
        if (this.HasValue)
        {
            var intermediate = mapper(this.Value);
            if (intermediate.HasValue)
                return MaybeFactory.Some(getResult(this.Value, intermediate.Value));
        }

        return MaybeFactory.None<TResult>();
    }

}

public static class MaybeFactory
{
    public static Maybe<T> Some<T>(T value) => new Maybe<T>(value, true);

    public static Maybe<T> None<T>() => new Maybe<T>(default(T), false);
}