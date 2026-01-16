using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using KaibaReduxAPI.Models;

namespace KaibaReduxAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ItemController : ControllerBase
    {
        [HttpGet("{id}", Name = "GetItem")] // Route = /api/item/2
        public ActionResult<Item> GetItem(int id)
        // takes a Item id as a url parameter and returns a Item object with the corresponding information 
        // the Item will contain pricelines
        {
            DbAccessManagement DAM = new DbAccessManagement();

            // get the Item
            Item item = DAM.GetItem(id);

            // if it's null, then the Item wasn't found
            if (item == null)
            {
                // return a 404 ERROR
                return NotFound();
            }
            else //otherwise return the Item
            {
                return item;
            }
        }

        [HttpPost] // Route = /api/item
        public IActionResult CreateItem(Item item)
        // POST request that takes JSON from the request body and builds a Item object
        // returns NoContent (204) if successful, returns server error (500)
        {
            DbAccessManagement DAM = new DbAccessManagement();
            bool result = DAM.InsertItem(item);
            if (result)
            {
                return StatusCode(201);
            }
            else
            {
                return StatusCode(500);
            }
        }

        [HttpDelete] // Route = DELETE /api/item
        // but uses the DELETE method (as opposed to the usual GET or POST
        public IActionResult DeleteItem(Item item)
        // takes a Item object from the JSON body and deletes that record
        // it only requires the id field, and ignores everything else
        // returns NotFound (404) if unsuccessful, returns NoContent (201) if successful
        {
            DbAccessManagement DAM = new DbAccessManagement();
            bool result = DAM.DeleteItem(item.Id);
            if (result)
            {
                return NoContent();
            }
            else
            {
                return NotFound();
            }
        }

        [HttpPut] // route = PUT /api/item 
        // Note that it uses PUT instead of GET or POST
        public IActionResult UpdateItem(Item item)
        {
            DbAccessManagement DAM = new DbAccessManagement();
            bool result = DAM.UpdateItem(item);
            if (result)
            {
                return NoContent();
            }
            else
            {
                return NotFound();
            }
        }
    }
}