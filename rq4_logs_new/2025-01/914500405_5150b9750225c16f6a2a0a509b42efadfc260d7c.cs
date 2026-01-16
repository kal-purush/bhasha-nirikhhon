namespace FactoryMethod.Extensions;

/// <summary>
/// Represents the enum extensions method class.
/// </summary>
internal static class EnumExtensionMethods
{
    /// <summary>
    /// Gets the description value of enum.
    /// </summary>
    /// <param name="enumValue">The <see cref="Enum"/> type.</param>
    /// <returns>The description value of enum.</returns>
    public static string GetDescription(this Enum enumValue)
    {
        var fieldInfo = enumValue.GetType().GetField(enumValue.ToString());

        if (fieldInfo is null) return string.Empty;

        var descriptionAttributes =
            (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);

        return descriptionAttributes.Length > 0 ? descriptionAttributes[0].Description : enumValue.ToString();
    }
}