using BusinessLayer.Interfaces;
using DatabaseLayer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BookStoreApp.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AddressController : ControllerBase
    {
        IAddressBL addressBL;
        public AddressController(IAddressBL addressBL)
        {
            this.addressBL = addressBL;
        }

        [HttpPost("addAddress/{UserId}")]
        public IActionResult AddAddress(int UserId, AddressModel addressModel)
        {
            try
            {
                var result = this.addressBL.AddAddress(UserId, addressModel);
                if (result.Equals(" Address Added Successfully"))
                {
                    return this.Ok(new { success = true, message = $"Address Added Successfully " });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = result });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [HttpPut("updateAddress/{AddressId}")]
        public IActionResult UpdateAddress(int AddressId, AddressModel addressModel)
        {
            try
            {
                var result = this.addressBL.UpdateAddress(AddressId, addressModel);
                if (result.Equals(true))
                {
                    return this.Ok(new { success = true, message = $"Address updated Successfully " });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = result });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        [HttpDelete("deletebook/{AddressId}")]
        public IActionResult DeleteAddress(int AddressId)
        {
            try
            {
                var result = this.addressBL.DeleteAddress(AddressId);
                if (result.Equals(true))
                {
                    return this.Ok(new { success = true, message = $"Address deleted Successfully " });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = result });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        [HttpGet("getaddressbyUserId/{UserId}")]
        public IActionResult GetAddressByAddressId(int UserId)
        {
            try
            {
                var result = this.addressBL.GetAddressByAddressId(UserId);
                if (result != null)
                {
                    return this.Ok(new { success = true, message = $"Address is Displayed Successfully by UserId ", response = result });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = $"Address id not exists " });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [HttpGet("getallbook")]
        public IActionResult GetAllAddress()
        {
            try
            {
                var result = this.addressBL.GetAllAddress();
                if (result != null)
                {
                    return this.Ok(new { success = true, message = $"All Address Displayed Successfully ", response = result });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = $"address are not there " });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

    }
}

