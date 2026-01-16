using FacadeAccountCreation.Core.Models.CreateAccount;
using System;
using System.Diagnostics.CodeAnalysis;

namespace FacadeAccountCreation.Core.Models.Organisations;

[ExcludeFromCodeCoverage]
public class LinkOrganisationModel
{
    public OrganisationModel Subsidiary { get; init; }
    public Guid ParentOrganisationId { get; init; }
    public Guid? UserId { get; set; }
}