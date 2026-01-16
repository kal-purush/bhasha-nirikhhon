using LanguageSchoolApp.core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanguageSchoolApp.model.Courses
{
    public class ClassPeriodSlot : ObservableObject
    {
        private bool _hasData;
        private ClassPeriod? _classPeriod;

        public bool HasData 
        { 
            get { return _hasData; }
            set
            { 
                _hasData = value;
                OnPropertyChanged();
            }
        }
        public ClassPeriod? Period 
        { 
            get { return _classPeriod; }
            set 
            { 
                _classPeriod = value; 
                OnPropertyChanged(); 
            }
        }

    }
}