using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Website_Ecommerce.API.Data.Entities;
using Website_Ecommerce.API.ModelDtos;
using Website_Ecommerce.API.Repositories;
using Website_Ecommerce.API.Response;

namespace Website_Ecommerce.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CartController:ControllerBase
    {   
        private readonly IMapper _mapper;
        private readonly ICartRepository _cartRepository;
        public CartController(ICartRepository cartRepository, IMapper mapper){
            _mapper = mapper;
            _cartRepository = cartRepository;
        }
        [HttpPost("Add-item-to-cart")]
        public async Task<IActionResult> AddItemToCart([FromBody]CartDto request, CancellationToken cancellationToken){
            var item = _mapper.Map<Cart>(request);
            _cartRepository.Add(item);
            var result = await _cartRepository.UnitOfWork.SaveAsync(cancellationToken);
            if(result == 0)
            {
                return BadRequest( new Response<ResponseDefault>()
                {
                    State = false,
                    Message = ErrorCode.ExcuteDB,
                    Result = new ResponseDefault()
                    {
                        Data = "Add ProductImage fail"
                    }
                });
            }
            return Ok( new Response<ResponseDefault>()
            {
                State = true,
                Message = ErrorCode.Success,
                Result = new ResponseDefault()
                {
                    Data = "Add ProductImage success"
                }
            });
        }

        [HttpPost("update-item-in-cart")]
        public async Task<IActionResult> UpdateItemCart([FromBody] CartDto request, CancellationToken cancellationToken)
        {
            
            var item = _mapper.Map<Cart>(request);
            _cartRepository.Update(item);
            var result = await _cartRepository.UnitOfWork.SaveAsync(cancellationToken);
            if(result == 0)
            {
                return BadRequest( new Response<ResponseDefault>()
                {
                    State = false,
                    Message = ErrorCode.ExcuteDB,
                    Result = new ResponseDefault()
                    {
                        Data = "Update Item to Cart fail"
                    }
                });
            }



            return Ok( new Response<ResponseDefault>()
            {
                State = true,
                Message = ErrorCode.Success,
                Result = new ResponseDefault()
                {
                    Data = "Update Item to Cart success"
                }
            });
        }
        [HttpDelete("Delete-Item-in-cart")]
        public async Task<IActionResult> DeleteItemInCart([FromQuery] int id){
            if(id.ToString() is null)
            {
                return BadRequest(null);
            }
            var item = await _cartRepository.Carts.FirstOrDefaultAsync(x => x.Id == id);
            if(item == null)
            {
                return BadRequest( new Response<ResponseDefault>()
                {
                    State = false,
                    Message = ErrorCode.NotFound,
                    Result = new ResponseDefault()
                    {
                        Data = "Delete item fail"
                    }
                });
            }

            _cartRepository.Delete(item);
            var result = await _cartRepository.UnitOfWork.SaveAsync();

            if(result > 0)
            {
                return Ok( new Response<ResponseDefault>()
                {
                    State = true,
                    Message = ErrorCode.Success,
                    Result = new ResponseDefault()
                    {
                        Data = item.Id.ToString()
                    }
                });
            }
            return BadRequest( new Response<ResponseDefault>(){
                State = false,
                Message= ErrorCode.ExistedDB,
                Result = new ResponseDefault(){
                    Data = "Delete item fail"
                }
            });
       }

       [HttpGet("Get-all-items-of-User")]
       public async Task<IActionResult> GetAllItemByIdUser(int id){
            var listItems = await _cartRepository.GetAllItemByIdUser(id);
            return Ok( new Response<ResponseDefault>()
            {
                State = true,
                Message = ErrorCode.Success,
                Result = new ResponseDefault()
                {
                    Data = listItems
                }
            });
       }
    }
}