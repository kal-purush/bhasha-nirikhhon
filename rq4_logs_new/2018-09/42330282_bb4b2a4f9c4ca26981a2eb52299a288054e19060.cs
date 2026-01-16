using System.Net;

namespace LG.Eloqua.Api.Rest.ClientLibrary.Models.Dtos
{
    public class CustomCampaignObjectDto
    {
        public long InstanceId { get; set; }
        public long ActivationId { get; set; }
        public HttpStatusCode Status { get; set; }
    }
}