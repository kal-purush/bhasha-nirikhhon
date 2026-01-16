using DiamondStoreSystem.Common.Enum;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiamondStoreSystem.DTO.EntitiesResponse
{
    public class CertificateResponse
    {
        public int GIAReportNumber { get; set; }
        public Shape Shaping { get; set; }
        public Grade CutGrade { get; set; }
        public float Height { get; set; }
        public float Width { get; set; }
        public float Length { get; set; }
        public float CaratWeight { get; set; }
        public ColorGrade ColorGrade { get; set; }
        public ClarityGrade ClarityGrade { get; set; }
        public Grade PolishGrade { get; set; }
        public Grade SymmetryGrade { get; set; }
        public Grade FluoresceneGrade { get; set; }
    }
}