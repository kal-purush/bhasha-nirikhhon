using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Vastra.API.Entities;
using Vastra.API.Models;
using Vastra.API.Models.ForCreationAndUpdate;
using Vastra.API.Services;

namespace Vastra.API.Controllers
{
    [ApiController]
    [Route("api/roles/{roleId}/users/{userId}/addresses")]
    public class AddressesController : Controller
    {
        private readonly IVastraRepository _vastraRepository;
        private readonly IMapper _mapper;
        const int maxAddressesPageSize = 10;
        public AddressesController(IVastraRepository vastraRepository, IMapper mapper)
        {
            _vastraRepository = vastraRepository;
            _mapper = mapper;
        }
        [HttpGet]
        [Authorize]
        public async Task<ActionResult<IEnumerable<AddressDto>>> GetAddresses(int roleId, int userId, int pageNumber = 1, int pageSize = 10)
        {
            if (!await _vastraRepository.RoleExistsAsync(roleId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.UserExistsWithRoleAsync(roleId, userId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.ValidateUserClaim(User, userId))
            {
                return Forbid();
            }
            if (pageSize > maxAddressesPageSize)
            {
                pageSize = maxAddressesPageSize;
            }
            var (addressEntities, paginationMetadata) = await _vastraRepository.GetAddressesForUserAsync(userId, pageSize, pageNumber);

            return Ok(_mapper.Map<IEnumerable<AddressDto>>(addressEntities));
        }
        [HttpHead("{addressId}")]
        [HttpGet("{addressId}", Name = "GetAddress")]
        [Authorize]
        public async Task<IActionResult> GetAddress(int roleId, int userId, int addressId)
        {
            if (!await _vastraRepository.RoleExistsAsync(roleId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.UserExistsWithRoleAsync(roleId, userId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.ValidateUserClaim(User, userId))
            {
                return Forbid();
            }
            var address = await _vastraRepository.GetAddressForUserAsync(userId, addressId);
            if (address == null)
            {
                return NotFound();
            }
            
            return Ok(_mapper.Map<AddressDto>(address));
            
        }
        [HttpPost]
        [Authorize]
        public async Task<ActionResult<AddressDto>> CreateAddress(int roleId, int userId, AddressForCreationDto address)
        {
            if (!await _vastraRepository.RoleExistsAsync(roleId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.UserExistsWithRoleAsync(roleId, userId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.ValidateUserClaim(User, userId))
            {
                return Forbid();
            }
            var finaladdress = _mapper.Map<Entities.Address>(address);

            finaladdress.DateAdded = DateTime.Now;
            finaladdress.DateModified = DateTime.Now;
            await _vastraRepository.AddAddressForUserAsync(userId, finaladdress);
            await _vastraRepository.SaveChangesAsync();

            var createdAddressToReturn = _mapper.Map<AddressDto>(finaladdress);
            return CreatedAtRoute("GetAddress",
                new
                {
                    roleId = roleId,
                    userId = userId,
                    orderId = createdAddressToReturn.AddressId
                },
                createdAddressToReturn

                );
        }

        [HttpDelete]
        [Authorize]
        public async Task<ActionResult> DeleteAddress(int roleId, int userId, int addressId)
        {
            if (!await _vastraRepository.RoleExistsAsync(roleId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.UserExistsWithRoleAsync(roleId, userId))
            {
                return NotFound();
            }
            if (!await _vastraRepository.ValidateUserClaim(User, userId))
            {
                return Forbid();
            }
            var addressToDelete = await _vastraRepository.GetAddressForUserAsync(userId, addressId);
            if (addressToDelete == null)
            {
                return NotFound();
            }
            _vastraRepository.DeleteAddress(addressToDelete);
            await _vastraRepository.SaveChangesAsync();
            return NoContent();
        }

    }
}