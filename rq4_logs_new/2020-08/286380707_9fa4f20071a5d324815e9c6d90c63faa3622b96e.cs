using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using APIPatrimonio.Models;
using APIPatrimonio.DAL;
using Microsoft.Extensions.Configuration;
using Dapper;

namespace APIPatrimonio.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class MarcasController : ControllerBase
    {
        //GET marcas - Obter todas as marcas
        [HttpGet]
        public List<Marca> GetMarcas([FromServices]IConfiguration conf)
        {
            return new MarcaRepository(conf).GetMarcas();
        } 

        //GET marcas/{id} - Obter uma marca por ID
        [HttpGet("{id}")]
        public Marca GetOnePatrimonio(long id,[FromServices]IConfiguration conf)
        {
            return new MarcaRepository(conf).GetSingleMarca(id);
        }

        //GET marcas/{id}/patrimônios – Obter todos os patrimônios de uma marca
        [HttpGet("{id}/patrimonios")]
        public List<Patrimonio> GetPatrimoniosMarca(long id,[FromServices]IConfiguration conf)
        {
            return new MarcaRepository(conf).GetPatrimoniosMarca(id);
        }

        //POST marcas - Inserir uma nova marca 
        public void PostMarca([FromBody]Marca NewMarca,[FromServices]IConfiguration conf)
        {
            new MarcaRepository(conf).InsertMarca(NewMarca);

        }

        //PUT marca/{id} - Alterar os dados de uma marca
        [HttpPut("{id}")]
        public void PutMarca(long id,[FromBody]Marca NewMarca,[FromServices]IConfiguration conf)
        {
            NewMarca.MarcaId = id;

            new MarcaRepository(conf).UpdateMarca(NewMarca);

        }

        //DELETE marca/{id} - Excluir uma marca 
        [HttpDelete("{id}")]
        public void DeleteMarca(long id,[FromServices]IConfiguration conf)
        {
           new MarcaRepository(conf).DeleteMarca(id);
        }

    }
}