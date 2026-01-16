using Application.DTO.Response;
using Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.DTO.Creator
{
    public class VariableFieldCreator
    {
        public VariableFieldResponse Create(VariableField field)
        {
            return new VariableFieldResponse()
            {
                Label = field.Label,
                Value = field.Value,
                DataTypeId = field.DataTypeId,
                DataTypeNav = field.DataTypeNav,
                ReportId = field.ReportId,
                ReportNav = field.ReportNav
            };
        }
    }
}