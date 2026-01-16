using System;
using System.Collections.Generic;
using contracted.Models;
using contracted.Services;
using Microsoft.AspNetCore.Mvc;

namespace contracted.Controllers
{
  [ApiController]
  [Route("/api/[controller]")]
  public class CompanysController : ControllerBase
  {
    private readonly CompanysService _cmps;
    public CompanysController(CompanysService cmps)
    {
      _cmps = cmps;
    }

    [HttpGet]
    public ActionResult<List<Company>> Get()
    {
      try
      {
           List<Company> comps = _cmps.Get();
           return Ok(comps);
      }
      catch (Exception err)
      {
          return BadRequest(err.Message);
      }
    }

        [HttpGet("{id}")]
    public ActionResult<Company> Get(int id)
    {
      try
      {
        Company comp = _cmps.Get(id);
        return Ok(comp);
      }
      catch (Exception err)
      {
        return BadRequest(err.Message);
      }
    }

        [HttpPost]

    public  ActionResult<Company> Create([FromBody] Company newComp)
    {
      try
      {
        Company created =  _cmps.Create(newComp);
        return Ok(created);
      }
      catch (Exception err)
      {
        return BadRequest(err.Message);
      }
    }

        [HttpDelete("{id}")]
    public ActionResult<Company> Delete(int id)
    {
      try
      {
        Company comp = _cmps.Delete(id);
        return Ok(comp);
      }
      catch (Exception err)
      {
        return BadRequest(err.Message);
      }
    }
  }
}