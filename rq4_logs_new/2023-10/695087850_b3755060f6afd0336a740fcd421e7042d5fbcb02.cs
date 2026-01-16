using System.ComponentModel.DataAnnotations;
using FoodImportLabelGenerator.Data;
using FoodImportLabelGenerator.Models;
using FoodImportLabelGenerator.Repository;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore; //needed because this class derives from ControllerBase

namespace FoodImportLabelGenerator.Controllers;

//SRP - Any controller class should focus solely on handling the HTTP requests and responses and should be as 'thin' as possible

[ApiController]
[Route("[controller]")]
public class UserController : ControllerBase
{
    private readonly ILogger<UserController> _logger;
    private readonly IConfiguration _configuration;
    private readonly IUserRepository _userRepository;

    public UserController(ILogger<UserController> logger, IConfiguration configuration, IUserRepository userRepository)
    {
        _logger = logger;
        _configuration = configuration;
        _userRepository = userRepository;
    }

    [HttpGet("GetAllAsync"), Authorize(Roles = "Admin")]
    public async Task<ActionResult<IEnumerable<User>>> GetAllAsync()
    {
        var users = _userRepository.GetAll();
        if (users == null)
        {
            return NotFound("There are no users in the database");
        }
        try
        {
            return Ok(users);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting users");
            return NotFound("Error getting users");
        }
    }

    [HttpGet("GetByUserNameAsync"), Authorize(Roles = "Admin")]
    public async Task<ActionResult<IEnumerable<User>>> GetByUserNameAsync([Required]string userName)
    {
        var users = _userRepository.GetByUserName(userName);

        if (!users.Any())
        {
            return NotFound($"Cannot find user with username {userName}.");
        }
        try
        {
            return Ok(users);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting users");
            return NotFound("Error getting users");
        }
    }

    [HttpGet("GetByEmailAsync/{email}"), Authorize(Roles = "Admin")]
    public async Task<ActionResult<User>> GetByEmailAsync(string email)
    {
        var user = _userRepository.GetByEmail(email);

        if (user == null)
        {
            return NotFound($"Cannot find user with email {email}.");
        }
        try
        {
            return Ok(user);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting user");
            return NotFound("Error getting user");
        }
    }
    
    [HttpGet("GetByIdAsync/{id}"), Authorize(Roles = "Admin")]
    public async Task<ActionResult<User>> GetByIdAsync(string id)
    {
        var user = _userRepository.GetById(id);

        if (user == null)
        {
            return NotFound($"Cannot find user with id {id}.");
        }
        try
        {
            return Ok(user);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting user");
            return NotFound("Error getting user");
        }
    }

        
    [HttpGet("GetByCompanyNameAsync/{companyName}"), Authorize(Roles = "Admin")]
    public async Task<ActionResult<User>> GetByCompanyNameAsync(string companyName)
    {
        var user = _userRepository.GetByCompanyName(companyName);

        if (user == null)
        {
            return NotFound($"Cannot find user with company name {companyName}.");
        }
        try
        {
            return Ok(user);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting user");
            return NotFound("Error getting user");
        }
    }

    
    [HttpPut("UpdateAsync/{id}"), Authorize(Roles = "User, Admin")]
    public async Task<ActionResult<User>> UpdateAsync(string id, string companyName, string firstName,
        string lastName, string phoneNumber)
    {
        User existingUser = _userRepository.GetById(id)!;

        if (existingUser == null)
        {
            return NotFound($"It is not possible to update user. There is no user with id {id}.");
        }
        
        existingUser.CompanyName = string.IsNullOrEmpty(companyName) ? existingUser.CompanyName : companyName;
        existingUser.FirstName = string.IsNullOrEmpty(firstName) ? existingUser.FirstName : firstName;
        existingUser.LastName = string.IsNullOrEmpty(lastName) ? existingUser.LastName : lastName;
        existingUser.PhoneNumber = string.IsNullOrEmpty(phoneNumber) ? existingUser.PhoneNumber : phoneNumber;

        _userRepository.Update(existingUser);

        return Ok(existingUser);
    }
    
    [HttpDelete("DeleteByIdAsync/{id}"), Authorize(Roles = "User, Admin")]
    public async Task<ActionResult<User>> DeleteByIdAsync(string id)
    {
        User existingUser = _userRepository.GetById(id)!;
        
        if (existingUser == null)
        {
            return NotFound($"It is not possible to delete user. There is no user with id {id}.");
        }

        _userRepository.Delete(existingUser);
        return Ok($"User with id {id} has been deleted successfully.");
    }
}