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
    public class WishListController : ControllerBase
    {
        IWishListBL wishListBL;
        public WishListController(IWishListBL wishListBL)
        {
            this.wishListBL = wishListBL;
        }

        [HttpPost("addBooksInWishList")]
        public IActionResult AddBookinWishList(AddToWishList wishListModel)
        {
            try
            {
                var result = this.wishListBL.AddBookinWishList(wishListModel);
                if (result.Equals("book is added in WishList successfully"))
                {
                    return this.Ok(new { success = true, message = $"Book is added in WishList  Successfully " });
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

        [HttpDelete("DeleteBookinWishList/{WishListId}")]
        public IActionResult DeleteBookinWishList(int WishListId)
        {
            try
            {
                var result = this.wishListBL.DeleteBookinWishList(WishListId);
                if (result.Equals(true))
                {
                    return this.Ok(new { success = true, message = $"Book is deleted from the WishList " });
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

        [HttpGet("GetAllBooksinWishList/{UserId}")]
        public IActionResult GetAllBooksinWishList(int UserId)
        {
            try
            {
                var result = this.wishListBL.GetAllBooksinWishList(UserId);
                if (result != null)
                {
                    return this.Ok(new { success = true, message = $"All Books Displayed in the WishList Successfully ", response = result });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = $"Books are not there in WishList " });
                }
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

    }
}