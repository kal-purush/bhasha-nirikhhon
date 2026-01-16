using BusinessCardInformation.Core.IServices;
using BusinessCardInformation.Core.Models.Request;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc; 

namespace ResturantWebSite.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController] 
    public class BusinessCardController : ControllerBase
    { 
        readonly IBusinessCardServices _baseServices;
        public BusinessCardController(IBusinessCardServices baseServices)
        { 
            _baseServices = baseServices;
        }

        [HttpPost]
        [Route("create")]
        public async Task<IActionResult> Create([FromBody] BusinessCardDTO productModule)
        {
           // await _baseServices.Create(productModule);
            var result = await _baseServices.Create(productModule);
            if (result ==null)
                return Ok();
            return BadRequest(result);
        }

        //[HttpPut]
        //[Route("update")]
        //public async Task<IActionResult> Update([FromBody] ProductModul productModule)
        //{
        //    productModule.UpdateBy = GetUserId();
        //    productModule.UpdateStamp = DateTime.Now;
        //    await _productServices.DeleteImage(productModule);
        //    await _baseServices.Update(productModule);
        //    return Ok(); 
        //}


        //[HttpGet]
        //[Route("getAll")]
        //public async Task< OperationResult<List<ProductModul>>> GetAll([FromQuery] ProductFilterModul productFilterModul)
        //{ 
        //    return await _productServices.GetAll(productFilterModul); 
        //}

        //[HttpGet]
        //[Route("getById/{id}")]
        //public async Task<IActionResult> GetById(int id)
        //{
            
        //    var item= await _productServices.GetById(id);
        //    if(item==null)
        //        return NotFound();
        //    return Ok( item);
        //}

        //[HttpDelete]
        //[Route("delete/{id}")]
        //public async Task<IActionResult> Delete(int id)
        //{
        //    var result = await _productServices.Delete(id);
        //    if (result.Success)
        //        return Ok(result);
        //    return BadRequest(result);
        //}
    }
}