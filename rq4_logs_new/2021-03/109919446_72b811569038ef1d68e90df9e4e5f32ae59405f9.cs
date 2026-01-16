using System;
using System.Diagnostics;

namespace Rocket.Surgery.Conventions
{
    /// <summary>
    /// An alternative to the [assembly: Convention] attribute, to export a convention from the class itself.
    /// </summary>
    /// <remarks>
    /// Only works with source generators enabled.
    /// </remarks>
    /// <seealso cref="Attribute" />
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    [Conditional("CodeGeneration")]
    public sealed class ExportConvention : Attribute
    {

    }
}