using AutoMapper;
using MagicVilla.Models;
using MagicVilla.Models.DTO;
using MagicVilla.Repository.IRepository;
using Microsoft.AspNetCore.JsonPatch;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace MagicVilla.Controllers
{
    // We need to apply [Route] attribute to API class.
    [Route("api/VillaNumber")]
    // Classes with [ApiController] are configured with features to improve developers experience for buildeing API.
    [ApiController]
    public class VillaNumberAPIController : ControllerBase
    {
        protected APIResponse _response;
        private readonly IUnitOfWork _unitOfWork;
        private readonly IMapper _mapper;
        public VillaNumberAPIController(IUnitOfWork unitOfWork, IMapper mapper)
        {
            _unitOfWork = unitOfWork;
            _mapper = mapper;
            _response = new();
        }

        [HttpGet]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<ActionResult<APIResponse>> GetVillaNumbers()
        {
            try
            {
                var villas = await _unitOfWork.VillaNumber.GetAllAsync();
                _response.Result = _mapper.Map<List<VillaNumberDTO>>(villas);
                _response.StatusCode = HttpStatusCode.OK;
                return Ok(_response);
            }
            catch (Exception ex)
            {
                _response.IsSuccess = false;
                _response.ErrorMessages = [ex.ToString()];
            }

            return _response;
        }

        [HttpGet("{id:int}", Name = "GetVillaNumber")]
        // Following attridutes specifies what status codes method can return.
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<APIResponse>> GetVillaNumber(int id)
        {
            try
            {
                if (id <= 0)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.BadRequest;
                    _response.ErrorMessages = ["id cannot be less or equal 0"];
                    return BadRequest(_response);
                }

                var villaNumber = await _unitOfWork.VillaNumber.GetAsync(v => v.VillaNo == id, tracked: false);
                if (villaNumber == null)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.NotFound;
                    _response.ErrorMessages = [$"Entity with Id {id} not found"];
                    return NotFound(_response);
                }

                _response.Result = _mapper.Map<VillaNumberDTO>(villaNumber);
                _response.StatusCode = HttpStatusCode.OK;
                return Ok(_response);
            }
            catch (Exception ex)
            {
                _response.IsSuccess = false;
                _response.ErrorMessages = [ex.ToString()];
            }

            return _response;
        }

        [HttpPost(Name = "CreateVillaNumber")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<ActionResult<APIResponse>> CreateVilla([FromBody] VillaNumberCreateDTO createDTO)
        {
            try
            {
                // With [ApiController] we dont need to explicitly check ModelState.
                // With [ApiController] validation occurs before entering method.
                //if (!ModelState.IsValid)
                //    return BadRequest(ModelState);

                if (createDTO is null)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.BadRequest;
                    _response.ErrorMessages = ["Argument is null"];
                    return BadRequest(_response);
                }
                
                VillaNumber villaNumber = _mapper.Map<VillaNumber>(createDTO);
                await _unitOfWork.VillaNumber.AddAsync(villaNumber);
                await _unitOfWork.SaveAsync();
                _response.Result = _mapper.Map<VillaNumberDTO>(villaNumber);
                _response.StatusCode = HttpStatusCode.Created;
                return CreatedAtRoute("GetVillaNumber", new { id = villaNumber.VillaNo }, _response);
            }
            catch (Exception ex)
            {
                _response.IsSuccess = false;
                _response.ErrorMessages = [ex.ToString()];
            }

            return _response;
        }

        [HttpDelete("{id:int}", Name = "DeleteVillaNumber")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<APIResponse>> DeleteVilla(int id)
        {
            try
            {
                if (id <= 0)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.BadRequest;
                    _response.ErrorMessages = ["Id cannot be less or equal 0"];
                    return BadRequest(_response);
                }
                
                var villaNumber = await _unitOfWork.VillaNumber.GetAsync(v => v.VillaNo == id);
                if (villaNumber == null)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.NotFound;
                    _response.ErrorMessages = [$"Entity with Id {id} not found"];
                    return NotFound(_response);
                }
                    
                _unitOfWork.VillaNumber.Remove(villaNumber);
                await _unitOfWork.SaveAsync();
                _response.StatusCode = HttpStatusCode.NoContent;
                _response.IsSuccess = true;
                return Ok(_response);
            }
            catch (Exception ex)
            {
                _response.IsSuccess = false;
                _response.ErrorMessages = [ex.ToString()];
            }

            return _response;
        }

        [HttpPut("{id:int}", Name = "UpdateVillaNumber")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<APIResponse>> UpdateVilla(int id, [FromBody] VillaNumberUpdateDTO updateDTO)
        {
            try
            {
                if (updateDTO is null || id != updateDTO.VillaNo)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.BadRequest;
                    if (updateDTO is null)
                        _response.ErrorMessages = ["Argument is null"];
                    else
                        _response.ErrorMessages = ["Input Id and Id in body must match"];

                    return BadRequest(_response);
                }
                
                var villaNumberFromDb = await _unitOfWork.VillaNumber.GetAsync(v => v.VillaNo == id, tracked: false);
                if (villaNumberFromDb == null)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.NotFound;
                    _response.ErrorMessages = [$"Entity with Id {id} not found"];
                    return NotFound(_response);
                }
 
                villaNumberFromDb = _mapper.Map<VillaNumber>(updateDTO);
                _unitOfWork.VillaNumber.Update(villaNumberFromDb);
                await _unitOfWork.SaveAsync();
                _response.StatusCode = HttpStatusCode.NoContent;
                _response.IsSuccess = true;
                return Ok(_response);
            }
            catch (Exception ex)
            {
                _response.IsSuccess = false;
                _response.ErrorMessages = [ex.ToString()];
            }

            return _response;
        }

        [HttpPatch("{id:int}", Name = "UpdatePartialVillaNumber")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<APIResponse>> UpdatePartialVilla(int id, JsonPatchDocument<VillaNumberUpdateDTO> patchDTO)
        {
            try
            {
                if (id <= 0 || patchDTO is null)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.BadRequest;
                    if (patchDTO is null)
                        _response.ErrorMessages = ["Argument is null"];
                    else
                        _response.ErrorMessages = ["Id cannot be less or equal 0"];

                    return BadRequest(_response);
                }
                
                var villaNumberFromDb = await _unitOfWork.VillaNumber.GetAsync(v => v.VillaNo == id, tracked: false);
                if (villaNumberFromDb == null)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.NotFound;
                    _response.ErrorMessages = [$"Entity with Id {id} not found"];
                    return NotFound(_response);
                }
                
                VillaNumberUpdateDTO updateDTO = _mapper.Map<VillaNumberUpdateDTO>(villaNumberFromDb);
                patchDTO.ApplyTo(updateDTO, ModelState);
                if (!ModelState.IsValid)
                {
                    _response.IsSuccess = false;
                    _response.StatusCode = HttpStatusCode.BadRequest;
                    _response.ErrorMessages = ["State of Model is not valid"];
                    return BadRequest(_response);
                }
                
                villaNumberFromDb = _mapper.Map<VillaNumber>(updateDTO);
                _unitOfWork.VillaNumber.Update(villaNumberFromDb);
                await _unitOfWork.SaveAsync();
                _response.StatusCode = HttpStatusCode.NoContent;
                _response.IsSuccess = true;
                return Ok(_response);
            }
            catch (Exception ex)
            {
                _response.IsSuccess = false;
                _response.ErrorMessages = [ex.ToString()];
            }

            return _response;
        }
    }
}