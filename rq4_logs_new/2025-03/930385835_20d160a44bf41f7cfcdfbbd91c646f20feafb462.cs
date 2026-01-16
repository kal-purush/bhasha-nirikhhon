using CSharpFunctionalExtensions;

namespace PetFamily.SharedKernel.ValueObjects.Ids;

public class AdminId : ValueObject, IComparable<AdminId>
{
    public Guid Value { get; }

    private AdminId(Guid value) => Value = value;

    public static AdminId NewAdminId() => new(Guid.NewGuid());
    public static AdminId EmptyAdminId => new(Guid.Empty);
    public static AdminId Create(Guid id) => new(id);
    
    protected override IEnumerable<IComparable> GetEqualityComponents()
    {
        yield return Value;
    }
    
    public static implicit operator AdminId(Guid AdminId) => new (AdminId);
    public static implicit operator Guid(AdminId AdminId) => AdminId.Value;

    public int CompareTo(AdminId? other)
    {
        if (other == null)
            throw new Exception("AdminId cannot be null");
        return Value.CompareTo(other.Value);
    }
}