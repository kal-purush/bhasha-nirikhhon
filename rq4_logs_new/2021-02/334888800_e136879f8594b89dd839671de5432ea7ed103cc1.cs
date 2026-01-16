using cuisin.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace cuisin.Controllers
{
    public class BusinessesController : ApiController
    {

        public IHttpActionResult Get()
        {
            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.Read();
                return Ok(bList);

            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
        }



        [HttpGet]
        [Route("api/Businesses/{category}")]
        public IHttpActionResult Get(string category)
        {
            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.Read(category);
                return Ok(bList);

            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
        }



        [HttpGet]
        [Route("api/Businesses/GetPromot/{category}")]
        public IHttpActionResult GetPromot(string category)
        {

            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.ReadPromot(category);
                return Ok(bList);

            } catch(Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
        }




        [HttpGet]
        [Route("api/Businesses/GetActive/{category}")]
        public IHttpActionResult GetActive(string category)
        {


            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.ReadActive(category);
                return Ok(bList);
            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
        }



        [HttpGet]
        [Route("api/Businesses/Byuser")]
        public IHttpActionResult GetByuser([FromUri] int[] att_id)
        {

            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.ReadByUser(att_id);
                return Ok(bList);
            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
        }


        [HttpGet]
        [Route("api/Businesses/Byuser/{category}")]
        public IHttpActionResult Get([FromUri] int[] att_id, string category)
        {
            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.ReadByUser(att_id, category);
                return Ok(bList);
            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
           
        }


        [HttpGet]
        [Route("api/Businesses/GetPromotByUser/{category}")]
        public IHttpActionResult GetPromotByUser([FromUri] int[] att_id, string category)
        {
            try
            {
                Businesses businesses = new Businesses();
                List<Businesses> bList = businesses.ReadPromotByUser(att_id, category);
                return Ok(bList);
            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);
            }
        }



        // GET api/<controller>/5
        public string Get(int id)
        {
            return "value";
        }


        // PUT api/<controller>/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/<controller>/5
        public void Delete(int id)
        {
        }
    }
}