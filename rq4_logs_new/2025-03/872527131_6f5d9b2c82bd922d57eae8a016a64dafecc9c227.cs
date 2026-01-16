using System;
using AutoMapper.Configuration.Conventions;

namespace Core.Entities;

/// <summary>
/// Base class for file-based entities providing common properties for file management
/// </summary>
public class BaseFileEntity
{
    /// <summary>
    /// Unique identifier for the entity
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Display name of the file
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// MIME type of the file
    /// </summary>
    public string ContentType { get; set; } = string.Empty;

    /// <summary>
    /// Size of the file in bytes
    /// </summary>
    public long FileSize { get; set; }

    /// <summary>
    /// UTC timestamp when the entity was created
    /// </summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// UTC timestamp when the entity was last updated
    /// </summary>
    public DateTime? UpdatedAt { get; set; }
}