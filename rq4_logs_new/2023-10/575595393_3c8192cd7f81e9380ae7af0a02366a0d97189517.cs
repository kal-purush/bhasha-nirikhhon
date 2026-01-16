using System;
using EssentialFrame.Domain.Core.ValueObjects.Core;

namespace EssentialFrame.ExampleApp.Domain.Posts.ValueObjects.Identifiers;

public class ImageIdentifier : TypedIdentifierBase<Guid>
{
    protected ImageIdentifier(Guid value) : base(value)
    {
    }

    public static implicit operator ImageIdentifier(Guid identifier)
    {
        return New(identifier);
    }

    public static implicit operator Guid(ImageIdentifier identifier)
    {
        return identifier.Value;
    }

    public static ImageIdentifier New(Guid value)
    {
        return new ImageIdentifier(value);
    }

    public override bool IsEmpty()
    {
        return Value == Guid.Empty;
    }
}