using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Dtos.Marca;
using api.Models;

namespace api.Mapper
{
    public static class MarcaMapper
    {
        public static MarcaDto ToMarcaDto(this Marca marcaModel)
        {
            return new MarcaDto
            {
                Id = marcaModel.Id,
                Str_nombre = marcaModel.Str_nombre,
                ProveedorId = marcaModel.ProveedorId,
                Productos = marcaModel.Productos.Select(m => m.ToProductoDto()).ToList()
            };
        }

        public static Marca ToMarcaFromCreate(this CreateMarcaRequestDto marcaDto, int proveedorId)
        {
            return new Marca
            {
                Str_nombre = marcaDto.Str_nombre,
                ProveedorId = proveedorId
            };
        }

        public static Marca ToMarcaFromUpdate(this UpdateMarcaRequestDto marcaDto)
        {
            return new Marca
            {
                Str_nombre = marcaDto.Str_nombre
            };
        }
    }
}