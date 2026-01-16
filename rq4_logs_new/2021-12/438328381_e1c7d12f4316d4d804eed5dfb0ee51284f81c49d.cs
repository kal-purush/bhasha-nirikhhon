using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using OngProject.Common;
using OngProject.Core.DTOs;
using OngProject.Core.Entities;
using OngProject.Core.Interfaces.IServices;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OngProject.Controllers
{
    [Route("[controller]")]
    [ApiController]

    public class ActivitiesController : ControllerBase
    {
        #region Object and Constructor
        private readonly IActivitiesServices _activitiesServices;
        public ActivitiesController(IActivitiesServices activitiesServices)
        {
            _activitiesServices = activitiesServices;
        }
        #endregion
        #region Documentacion
        /// <summary>
        /// Endpoint para obtener todas las entradas de Activities. Se debe ser ADMINISTRADOR
        /// </summary>
        /// <response code="200">Solicitud concretada con exito</response>
        /// <response code="401">Credenciales no validas</response> 
        #endregion
        [HttpGet]
        public async Task<IEnumerable<ActivitiesDTO>> GetAll()
        {
            return await _activitiesServices.GetAll();
        }
        #region Documentacion
        /// <summary>
        /// Endpoint para obtener una activity por id. Se debe ser ADMINISTRADOR
        /// </summary>
        /// <response code="200">Solicitud concretada con exito</response>
        /// <response code="401">Credenciales no validas</response> 
        #endregion
        [HttpGet("{id}")]
        public async Task<IActionResult> Get(int id)
        {
            if(!_activitiesServices.EntityExists(id))
            {
                return NotFound();
            }
            var activity = await _activitiesServices.GetById(id);
            return Ok(activity);
        }
        

    }
}