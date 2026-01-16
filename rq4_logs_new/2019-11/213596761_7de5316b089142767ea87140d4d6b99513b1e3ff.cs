using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using servicer.API.Data;
using servicer.API.Dtos;
using servicer.API.Helpers;
using servicer.API.Models;

namespace servicer.API.Controllers
{
    [ServiceFilter(typeof(SetLastActive))]
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class NotesController : ControllerBase
    {
        private readonly IServicerRepository _repository;
        private readonly IMapper _mapper;
        public NotesController(IServicerRepository repository, IMapper mapper)
        {
            _repository = repository;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<IActionResult> GetNotes()
        {
            var notes = await _repository.GetNotes();
            var notesToReturn = _mapper.Map<IEnumerable<NoteForListDto>>(notes);

            return Ok(notesToReturn);
        }

        [HttpGet("{id}", Name = "GetNote")]
        public async Task<IActionResult> GetNote(int id)
        {
            var note = await _repository.GetNote(id);
            var noteToReturn = _mapper.Map<NoteForDetailDto>(note);

            return Ok(noteToReturn);
        }

        [HttpPost]
        public async Task<IActionResult> CreateNote(NoteForSendDto noteForSendDto)
        {
            var noteToCreate = _mapper.Map<Note>(noteForSendDto);

            var createdNote = await _repository.CreateNote(noteToCreate);
            var noteToReturn = _mapper.Map<NoteForDetailDto>(createdNote);

            return CreatedAtRoute("GetNote", new { controller = "Notes", id = createdNote.Id }, noteToReturn);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateNote(int id, NoteForUpdateDto noteForUpdateDto)
        {
            var noteFromRepo = await _repository.GetNote(id);
            _mapper.Map(noteForUpdateDto, noteFromRepo);

            if (await _repository.SaveAll())
                return NoContent();

            throw new Exception($"Błąd przy edytowaniu notatki o id: {id}.");
        }
    }
}