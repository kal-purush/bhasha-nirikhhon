using BLL.Converters.Interface;
using SchedulerModels;
using SchedulerViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BLL.Converters
{
    public class ChiefConverter : IChiefConverter
    {
        private readonly IStudentConverter StudentConverter;
        public ChiefConverter(IStudentConverter studentConverter)
        {
            StudentConverter = studentConverter ?? throw new ArgumentNullException(nameof(studentConverter));
        }
        public Chief ConvertToEntity(ChiefViewModel model)
        {
            Chief user = new Chief(StudentConverter.ConvertToEntity(model));
            user.Profile = model.Profile;
            return user;
        }

        public ChiefViewModel ConvertToViewModel(Chief entity)
        {
            ChiefViewModel user = new ChiefViewModel(StudentConverter.ConvertToViewModel(entity));
            user.Profile = entity.Profile;
            return user;
        }
    }
}