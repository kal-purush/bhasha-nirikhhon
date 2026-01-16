using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Prism.Mvvm;

namespace TodoManager2.model {
    class Tag : BindableBase{

        private string content;
        public string Content {
            get => content;
            set {
                content = value;
                RaisePropertyChanged();
            }
        }

        private int id;
        public int ID {
            get => id;
            set {
                id = value;
                RaisePropertyChanged();
            }
        }

        private bool isChecked;
        public bool IsChecked {
            get => isChecked;
            set {
                isChecked = value;
                RaisePropertyChanged();
            }
        }
    }
}