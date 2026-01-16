using ch.hsr.wpf.gadgeothek.domain;
using ch.hsr.wpf.gadgeothek.service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace ch.hsr.wpf.gadgeothek.ui.ViewModel
{
    class GadgetViewModel : BindableBase
    {
        private LibraryAdminService Service { get; set; }

        public List<Gadget> Gadgets { get; set; }
        public List<Loan> Loans { get; set; }

        public Gadget SelectedGadget { get; set; }

        public ICommand EditGadgetCommand { get; set; }
        public ICommand DeleteGadgetCommand { get; set; }

        public GadgetViewModel()
        {
            var url = System.Configuration.ConfigurationManager.AppSettings["serverGadgeothek"];
            Service = new LibraryAdminService(url);
            Gadgets = Service.GetAllGadgets();
            Loans = Service.GetAllLoans();

            EditGadgetCommand = new RelayCommand.RelayCommand<Object>((o) => EditGadget());
            DeleteGadgetCommand = new RelayCommand.RelayCommand<Object>((o) => DeleteGadget());
        }

        public void EditGadget()
        {
            
        }

        public void DeleteGadget()
        {
            if(SelectedGadget == null)
            {
                MessageBox.Show("Please select the gadget first which you want to delete.");
            }
            else
            {
                DeleteGadget deleteGadget = new DeleteGadget(SelectedGadget);
                deleteGadget.Show();
            }
        }
    }
}