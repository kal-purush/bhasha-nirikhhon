using Microsoft.AspNetCore.Routing;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Community.EncryptionPropertyEditor.Controllers;
using Umbraco.Extensions;

namespace Umbraco.Community.EncryptionPropertyEditor.NotificationHandlers;

public class VariableParserNotificationHandler : Cms.Core.Events.INotificationHandler<ServerVariablesParsingNotification>
{
    private readonly LinkGenerator _linkGenerator;

    public VariableParserNotificationHandler(LinkGenerator linkGenerator)
    {
        _linkGenerator = linkGenerator;
    }

    public void Handle(ServerVariablesParsingNotification notification)
    {
        var variables = notification.ServerVariables;

        if (!variables.TryGetValue(Constants.Area, out var encryptedObject))
            encryptedObject = new Dictionary<string, object>();

        if (encryptedObject is not Dictionary<string, object> typedEncryptedObject)
            typedEncryptedObject = new Dictionary<string, object>();

        if (!typedEncryptedObject.ContainsKey(Constants.EncryptionApi))
            typedEncryptedObject.Add(Constants.EncryptionApi,
                _linkGenerator.GetUmbracoApiServiceBaseUrl<EncryptionApiController>(c => c.Ping()) ?? string.Empty
            );

        variables[Constants.Area] = typedEncryptedObject;
    }
}