using System.Security.Claims;
using CollectionManagementAPI.Entity;
using CollectionManagementAPI.Entity.Models.Collections;
using CollectionManagementAPI.Entity.Transformation;
using CollectionManagementAPI.Service.Interfeces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;

namespace Collection_Management_API.Controllers;

[ApiController]
[SwaggerTag("Collection")]
[Route("Collection")]
[Authorize (Roles = "Admin")]

public class CollectionController : Controller
{
    private readonly ICollectionService _collectionService;
    private readonly IUserService _userService;

    public CollectionController(ICollectionService collectionService, IUserService userService)
    {
        _collectionService = collectionService;
        _userService = userService;
    }
    
    [HttpGet("{id:int}")]
    public async Task<ActionResult<CollectionEntity>> GetById(int id) 
    {
        var collection = await _collectionService.GetById(id);
        if (collection == null)
        {
            return NotFound("Collection not found");
        }
        return collection;
    }

    [HttpGet("")]
    public ActionResult<IQueryable<CollectionEntity>> GetAll()
    {
        var collections = _collectionService.GetAll();
        return Ok(collections);
    }

    [HttpGet("{skip:int}/{take:int}")]
    public ActionResult<IQueryable<CollectionEntity>> GetPeriod(int skip, int take)
    {
        var collections = _collectionService.GetPeriod(skip, take);
        return Ok(collections);
    }

    [HttpPost("")]
    public async Task<ActionResult<CollectionEntity>> Create(CollectionCreateModel createModel)
    {
        var name = this.HttpContext.User.Claims
            .FirstOrDefault(x => x.Type == ClaimTypes.Name);

        var user = _userService.SearchByLogin(name.Value);
        
        var collection = new CollectionEntity()
        {
            Name = createModel.Name, 
            Description= createModel.Description,
            Topic = createModel.Topic,
            UserId = user.Id
        };
        await _collectionService.Create(collection);
        return Ok(collection.ToCollectionModel());
    }
    
    [HttpPut("")]
    public async Task<ActionResult<UserEntity>> Update(CollectionModel collectionModel)
    {
        var name = this.HttpContext.User.Claims
            .FirstOrDefault(x => x.Type == ClaimTypes.Name);

        var user = _userService.SearchByLogin(name.Value);
        
        var collection = await _collectionService.GetById(collectionModel.Id);
        
        if (user.Id != collection.UserId)
        {
            return BadRequest("This is not your collection");
        }
        
        collection.Name = collectionModel.Name;
        collection.Description = collectionModel.Description;
        collection.Topic = collectionModel.Topic;

        await _collectionService.Update(collection);
        return Ok(collection.ToCollectionModel());
    }
    
    [HttpDelete("{id:int}")]
    public async Task<bool> Delete(int id)
    {
        return await _collectionService.Delete(id);
    }

    [HttpGet("{name}")]
    public async Task<ActionResult<CollectionEntity>> SearchByName(string name)
    {
        var collection = await _collectionService.SearchByName(name);
        if (collection == null)
        {
            return NotFound();
        }

        return Ok(collection);
    }
}