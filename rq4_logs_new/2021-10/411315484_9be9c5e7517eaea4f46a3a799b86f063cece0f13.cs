using AliChoixServer.Database;
using AliChoixServer.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AliChoixServer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        private readonly MongoDbService m_mongoCrud;
        public ProductController()
        {
            m_mongoCrud = new MongoDbService();
        }

        /// <summary>
        /// Informations on a product
        /// </summary>
        /// <response code="200">Product found</response>
        /// <response code="404">Product not found</response>
        [HttpGet]
        public ActionResult<Product> Get(String universalProductCode)
        {
            Product product = m_mongoCrud.LoadDocumentById<Product>(universalProductCode);

            return product == null ? NotFound() : product;
        }

        /// <summary>
        /// Add product to Alichoix Database
        /// </summary>
        /// <response code="200">Product correctly analysed</response>
        /// <response code="400">Bad request</response>
        [HttpPost]
        public ActionResult<Product> Post(String universalProductCode, IFormFile image)
        {
            var httpRequest = ControllerContext.HttpContext.Request;

            if (universalProductCode == null || image == null) return BadRequest();

            //todo request the vision service to analyse the image
            //todo create a product from response
            //todo add the product to the db
            //todo return the product created

            return new Product();
        }
    }
}