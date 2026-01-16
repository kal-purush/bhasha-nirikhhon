using Azure.Storage.Blobs.Models;
using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Photos.Application.Commands.Photos;
using Photos.Application.Queries.Photos;
using Photos.Domain.Blob;
using Photos.Domain.DataBaseEntity;
using Photos.Domain.Repositories;
using Photos.Infrastructure.Service.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Photos.Controllers
{
    [ApiController]
    [Route("/api/[controller]")]
    public class RedisController : Controller
    {
        private readonly IEventService _eventService;

        public RedisController(IEventService eventService)
        {
            _eventService = eventService;
        }

        [HttpGet]
        public async Task<IEnumerable<EventEntity>> Get()
        {
            return await _eventService.GetAllEvents();
        }

       
    }
}