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
    public class KaibaController : ControllerBase
        // this is the main controller for the API routing
    {
        // route path
        // GET api/kaiba/
        [HttpGet]
        public ActionResult<List<Menu>> GetKaiba()
            // returns a list of the menus
        {
            // create DbAccessManagement object
            DbAccessManagement DAM = new DbAccessManagement();
            // return the list of menus
            return DAM.getMenus();



            // This is all test code
            /*
            Menu testMenu = new Menu(1, "NEW_MENU", "DESCRIPTIONDFSJGH", 30000);
            DAM.InsertMenu(testMenu);
            DAM.DeleteMenu(3);

            Section testSection = new Section(1, "SECTION_TEST", "MY_LOVELY_SECTION_DESCRIPT", 30000, "NO_PICS", 1);
            DAM.InsertSection(testSection);
            DAM.DeleteSection(5);

            Item testItem = new Item(1, "ITEM_NAME", "ITEM_DESC", 20000, "LOVELY_PICTURE_OF_SOUP", 1);
            DAM.InsertItem(testItem);
            DAM.DeleteSection(9);

            Priceline testPrice = new Priceline(1, "68 Orders", (decimal) 100.99, 10000, 1);
            DAM.InsertPriceline(testPrice);
            DAM.DeletePriceline(11);


            return DAM.GetSectionsInMenu(1);
            */
        }

        [HttpGet("{id}")] // Route = /api/kaiba/2
        public ActionResult<Menu> GetKaiba(int id)
        // takes a menu id as a url parameter and returns a menu object with the corresponding information 
        // the menu will contain sections, which contain items, which contain pricelines
        {
            DbAccessManagement DAM = new DbAccessManagement();

            // get the sections in that menu
            Menu menu = DAM.getMenu(id);

            // if the list is null, then the menu wasn't found
            if (menu == null)
            {
                // return a 404 ERROR
                return NotFound();
            }
            else //otherwise return the menu
            {
                return menu;
            }
        }

        [HttpPost("/menu")] // Route = /api/kaiba/menu
        public IActionResult CreateKaiba(Menu menu)
            // POST request that takes JSON from the request body and builds a Menu object
            // returns NoContent (204) if successful, returns server error (500)
        {
            DbAccessManagement DAM = new DbAccessManagement();
            bool result = DAM.InsertMenu(menu);
            if (result)
            {
                return StatusCode(201);
            }
            else
            {
                return StatusCode(500);
            }
        }
    }
}