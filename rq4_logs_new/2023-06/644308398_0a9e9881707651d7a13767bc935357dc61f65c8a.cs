using EasyMicroservices.Cores.AspCoreApi;
using EasyMicroservices.Cores.Database.Interfaces;
using EasyMicroservices.TemplateGeneratorMicroservice.Contracts.Common;
using EasyMicroservices.TemplateGeneratorMicroservice.Database.Entities;

namespace EasyMicroservices.TemplateGeneratorMicroservice.WebApi.Controllers
{
    public class FormDetailController : SimpleQueryServiceController<FormDetailEntity, long, FormDetailContract, FormDetailContract, FormDetailContract>
    {
        public FormDetailController(IContractLogic<FormDetailEntity, FormDetailContract, FormDetailContract, FormDetailContract, long> contractReadable) : base(contractReadable)
        {

        }
    }
}