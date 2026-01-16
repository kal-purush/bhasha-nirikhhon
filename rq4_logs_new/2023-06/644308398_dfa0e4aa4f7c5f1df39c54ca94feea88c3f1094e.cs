using EasyMicroservices.Cores.AspCoreApi;
using EasyMicroservices.Cores.Database.Interfaces;
using EasyMicroservices.TemplateGeneratorMicroservice.Contracts.Common;
using EasyMicroservices.TemplateGeneratorMicroservice.Contracts.Requests;
using EasyMicroservices.TemplateGeneratorMicroservice.Database.Entities;

namespace EasyMicroservices.TemplateGeneratorMicroservice.WebApi.Controllers
{
    public class FormValuesController : SimpleQueryServiceController<FormFilledEntity, long, FormValuesRequestContract, FormContract, FormContract>
    {
        public FormValuesController(IContractLogic<FormFilledEntity, FormValuesRequestContract, FormContract, FormContract, long> contractReadable) : base(contractReadable)
        {

        }
    }
}