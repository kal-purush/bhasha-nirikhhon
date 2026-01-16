namespace CocktailBar.Domain.Seedwork;

/// <summary>
/// Base class for domain objects that have a natural identity and are part of an aggregate.
/// </summary>
/// <typeparam name="TEntity">The type of the entity.</typeparam>
// TODO: AI generate code
public abstract class NaturalKeyEntity<TEntity>
    where TEntity : NaturalKeyEntity<TEntity>
{
    private readonly Func<TEntity, object> _naturalKeyAccessor;

    /// <summary>
    /// Initializes a new instance of the <see cref="NaturalKeyEntity{TEntity}"/> class.
    /// </summary>
    /// <param name="naturalKeyAccessor">The natural identity of the entity.</param>
    protected NaturalKeyEntity(Func<TEntity, object> naturalKeyAccessor) => _naturalKeyAccessor = naturalKeyAccessor;

    /// <summary>
    /// Determines whether the specified object is equal to the current object.
    /// </summary>
    /// <param name="obj">The object to compare with the current object.</param>
    /// <returns>true if the specified object is equal to the current object; otherwise, false.</returns>
    public override bool Equals(object? obj)
    {
        if (obj is not NaturalKeyEntity<TEntity> other)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        var thisKey = _naturalKeyAccessor((TEntity)this);
        var otherKey = _naturalKeyAccessor((TEntity)other);
        return thisKey.Equals(otherKey);
    }

    /// <summary>
    /// Serves as the default hash function.
    /// </summary>
    /// <returns>A hash code for the current object.</returns>
    public override int GetHashCode()
    {
        var naturalKey = _naturalKeyAccessor((TEntity)this);
        return naturalKey?.GetHashCode() ?? 0;
    }
}