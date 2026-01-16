using Medical_record.Data.Models;
using System.Windows.Forms;

namespace Medical_record.Abstractions
{
    public interface IAppController
    {
        IDataContext DataContext { get; }
        IMessageService MessageService { get; }

        Form GetMainForm();
        UserControl GetUcViewInput(string key);
        UserControl GetUcViewOutput(string key);
        void ShowAddMedicationsView();
        void ShowAddMedicationsView(Medications medications);
        void ShowAddProceduresView();
        void ShowAddProceduresView(Procedure procedure);
        void ShowCardView();
        void ShowDiagnosesView();
        void ShowDiagnosisView();
        void ShowDiagnosisView(Diagnosis diagnosis);
        void ShowDoctorsView();
        void ShowMedicationsView();
        void ShowProceduresView();
        void ShowRegistrationView();
        void ShowRegistrationView(Patient patient);
    }
}