
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Green.Context;
using Green.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Green.Controllers
{
    // need to remove authorize to post a new post or login in front <--
    // [Authorize]
    [ApiController]
    [Route("[controller]")]
    public class ProductsController : ControllerBase
    {
        private readonly Greencontext greencontext;


        public ProductsController(Greencontext greencontext)
        {

            this.greencontext = greencontext;

        }


        // all users can see all the comments
        // [AllowAnonymous]
        [HttpGet("Products/{productId}")]
        public async Task<IActionResult> Get(int productId)
        {
            // SResponse<Product> SResponse = new SResponse<Product>();

            Product dbProduct = await greencontext.Products.FirstOrDefaultAsync(p => p.id == productId);
            //  SResponse.Data = dbProduct;
            if (dbProduct == null)
            {
                return NotFound();
            }

            return Ok(dbProduct);
        }


        //    // [AllowAnonymous]
        //     [HttpGet("{id}")]
        //     public async Task<IActionResult> GetSingel(int id)
        //     {
        //         return Ok(await _commentService.GetCommentById(id));
        //     }

        //  [AllowAnonymous]
        [HttpPost]
        public async Task<IActionResult> AddProduct(Product newProduct)
        {
          
            
               // SResponse<List<Product>> Response = new SResponse<List<Product>>();
                // try
                // {
                    List<Product> dbProduct = await greencontext.Products.ToListAsync();
                   // newComment.Post = await _context.Posts.FindAsync(newComment.Post.Id);

                    await greencontext.Products.AddAsync(newProduct);
                    await greencontext.SaveChangesAsync();

                 //   Response.Data = dbProduct;

                // }
                // catch (Exception ex)
                // {
                //     Response.Sucsses = false;
                //     Response.Message = ex.Message;
                // }
               return Ok(dbProduct);

            
        }


        //   //  [AllowAnonymous]
        //     [HttpPut("{id}")]
        //     public async Task<IActionResult> UpdatePost(Comment UpdatedComment, int id)
        //     {

        //         ServiceResponse<Comment> responce = await _commentService.UpdateComment(UpdatedComment, id);

        //         if (responce.Data == null)
        //         {
        //             return NotFound(responce);
        //         }

        //         return Ok(responce);

        //     }

        //     //[AllowAnonymous]
        //     [HttpDelete("{id}")]
        //     public async Task<IActionResult> Delete(int id)
        //     {
        //         ServiceResponse<List<Comment>> responce = await _commentService.DeleteComment(id);

        //         if (responce.Data == null)
        //         {
        //             return NotFound(responce);
        //         }

        //         return Ok(responce);
        //     }


    }
}