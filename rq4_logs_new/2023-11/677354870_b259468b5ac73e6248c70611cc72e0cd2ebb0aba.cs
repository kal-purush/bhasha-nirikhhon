using Microsoft.AspNetCore.Mvc;
using Mod4.Lection4.Hw1.Application.DTOs;
using Mod4.Lection4.Hw1.Domain.Interfaces;
using Mod4.Lection4.Hw1.Domain.Models;


namespace Mod4.Lection4.Hw1.Application.Controllers;

[Route("api/[controller]")]
[ApiController]
public class AddressController : ControllerBase  // депенденсі інжекшен
{
    private readonly IManagerRepository _managerRepository;
    public AddressController(IManagerRepository managerRepository) => _managerRepository = managerRepository;


    [HttpGet]
    public async Task<ActionResult> GetAllAsync()
    {
        var addresses = await _managerRepository.AddressRepository.GetAllAddressesAsync();
        
        if (addresses == null) return NotFound();

        return Ok(addresses.OrderBy(p => p.Street));
    }

    [HttpGet("id")]
    public async Task<IActionResult> GetByIdAsync(Guid addressId)
    {
        var staffMember = await _managerRepository.AddressRepository.GetAddressAsync(addressId);
        
        if (staffMember == null) return NotFound();
        
        return Ok(staffMember);
    }

    [HttpPost]
    public async Task<IActionResult> PostAsync([FromBody] AddressDto addressProperty)
    {
        var addressId = Guid.NewGuid();
        
        var address = new Address
        {
            Id = addressId,
            Street = addressProperty.Street,
            City = addressProperty.City
        };
        
        await _managerRepository.AddressRepository.CreateAddressAsync(address);
        await _managerRepository.SaveChangesRepository.SaveChangesAsync();

        return Ok(address);
    }

    [HttpDelete("id")]
    public async Task<IActionResult> DeleteAsync(Guid addressId)
    {
        var address = await _managerRepository.AddressRepository.GetAddressAsync(addressId);
        
        if (address == null) return NotFound();
        
        await _managerRepository.AddressRepository.DeleteAddressAsync(address);
        await _managerRepository.SaveChangesRepository.SaveChangesAsync();

        return Ok(address);
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> ChangeAsync([FromRoute] Guid id, [FromBody] AddressDto addressDto)
    {
        var address = await _managerRepository.AddressRepository.GetAddressAsync(id);
        
        if (address == null) return NotFound();

        address.Street = addressDto.Street;
        address.City = addressDto.City;

        await _managerRepository.SaveChangesRepository.SaveChangesAsync();

        return Ok(address);
    }

    // 1-1
    [HttpPut("{id}/user")]
    public async Task<IActionResult> AddClassAsync([FromRoute] Guid id, UserDto user)
    {

        await _managerRepository.AddressRepository.AddUserAsync(id, 
            new User
            {
                Username = user.Username
            });

        await _managerRepository.SaveChangesRepository.SaveChangesAsync();

        return Ok();
    }
}