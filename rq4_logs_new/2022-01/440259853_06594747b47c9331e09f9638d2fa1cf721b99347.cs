using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MyAPI.DTOs;
using MyAPI.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MyAPI.Controllers
{
    [Route("api/rap")]
    [ApiController]
    public class MovieTheaterController : ControllerBase
    {
        private readonly MyDbContext _db;
        private readonly IMapper _mapper;

        public MovieTheaterController(MyDbContext db, IMapper mapper)
        {
            _db = db;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<ActionResult<List<MovieTheater>>> Get()
        {
            var result = await _db.MovieTheaters.ToListAsync();

            return Ok(_mapper.Map<List<MovieTheaterDTO>>(result));
        }

        [HttpGet]
        public async Task<ActionResult<List<MovieTheater>>> GetById(int id)
        {
            var result = await _db.MovieTheaters.FindAsync(id);

            if (result != null)
                return Ok(_mapper.Map<MovieTheaterDTO>(result));
            return NotFound();

        }

        [HttpPost]
        public async Task<ActionResult> Post(MovieTheaterCreationDTO movieTheaterCreationDTO)
        {
            var result = _mapper.Map<MovieTheater>(movieTheaterCreationDTO);

            _db.Add(result);
            await _db.SaveChangesAsync();
            return Ok();
        }

        [HttpPut("{id:int}")]
        public async Task<ActionResult> Put(int id, MovieTheaterCreationDTO movieTheaterCreationDTO)
        {
            var result = await _db.MovieTheaters.FindAsync(id);
            if (result != null)
            {
                _mapper.Map(movieTheaterCreationDTO, result);
                await _db.SaveChangesAsync();
                return Ok();
            }
            else
            {
                return NotFound();
            }
        }

        [HttpDelete]
        public async Task<ActionResult> Del(int id)
        {
            var del = await _db.MovieTheaters.FindAsync(id);
            if (del != null)
            {
                _db.Remove(del);
                await _db.SaveChangesAsync();
                return Ok();
            }
            else
            {
                return NotFound();
            }

        }
    }
}