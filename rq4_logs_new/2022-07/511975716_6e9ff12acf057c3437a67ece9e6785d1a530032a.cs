using Microsoft.AspNetCore.Mvc;
using NimmalaTicketSales.Models;

namespace NimmalaTicketSales.Controllers
{
    //Created by Naveen Nimmala
    //888888888888888888


    public class CartController : Controller
    {
        public IActionResult Buy(int id)
        {
            // gets the id of the event that the user wants to buy the ticket for
            //then using EventsService gets an object representing the event

            EventService eventService = new EventService();
            Event selectedEvent =eventService.GetEvent(id);

            //start sale event by creating Sale object ans setting name of the event and ticket price.
            //(constructor ot the sale object)

            Sale newSale=new Sale(selectedEvent.Title,selectedEvent.TicketPrice);

            return View(newSale);   

        }//Buy()
        public IActionResult Confirmation(Sale model)
        {
            //The binding model contains the form data for buying the ticket

            if(ModelState.IsValid)
            {
                model.ProcessSale();//call the SaleObject method to calculate sale price

                //pass the saleObject to the Confirmation view as view model to display Sale confirmation

                return View(model);

            }//modelValid
            else
            {
                return View("Buy",model);
            }//model not valid(Errors)


        }//Confirmation

    }//CartController

}//namespace