using CaptoneProject_IOTS_BOs.DTO.MaterialDTO;
using CaptoneProject_IOTS_BOs.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.CommonDTO
{
    public class CommonSearchRequestDTO
    {
        [Required]
        public string Keyword { set; get; } = "";
    }

    public class CommonSearchResponseDTO
    {
        public List<IotsDevice>? IotDevices { set; get; }

        public List<Combo>? Combos { set; get; }
    }
}