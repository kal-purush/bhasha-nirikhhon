using backend_carhelp.Workshop_management.Domain.Model.Commands;
using backend_carhelp.Workshop_management.Interfaces.REST.Resources;

namespace backend_carhelp.Workshop_management.Interfaces.REST.Transform;

public static class CreateAdvertasingCommandFromResourceAssembler
{
    public static CreateAdvertasingCommand ToCommandFromResource(CreateAdvertasingResource resource) =>
        new CreateAdvertasingCommand(resource.Name, resource.ImageUrl, resource.Slogan, resource.Message, resource.WorkshopId);
}