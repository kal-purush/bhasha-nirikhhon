using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using Play.Catalog.Service.Dtos;

namespace Play.Catalog.Service.Controller
{
  [ApiController]
  [Route("items")]
  public class ItemsController : ControllerBase
  {
    private static readonly List<ItemDto> items = new()
    {
      new ItemDto(Guid.NewGuid(), "Postion", "Resports a small amount od HP", 5, DateTimeOffset.UtcNow),
      new ItemDto(Guid.NewGuid(), "Antidote", "Cures poison", 7, DateTimeOffset.UtcNow),
      new ItemDto(Guid.NewGuid(), "Bronze sword", "Deals a small amount od damage", 20, DateTimeOffset.UtcNow)
    };

    [HttpGet]
    public IEnumerable<ItemDto> Get()
    {
      return items;
    }

    [HttpGet("{id}")]
    public ItemDto GetById(Guid id)
    {
      var item = items.Where(item => item.Id == id).SingleOrDefault();
      return item;
    }
  }
}