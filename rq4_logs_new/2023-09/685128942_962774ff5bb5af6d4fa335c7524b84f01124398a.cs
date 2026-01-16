using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;



namespace RAD_Project
{
    class FormController
    {
        private FormController() {
            if (form == null)
                throw new Exception("Set Static Form property First");
        }
        private static Form1 form=null;
        private static FormController instance;
        public static FormController Instance
        {
            get
            {
                if (instance == null) instance = new FormController();
                return instance;
            }
        }
        public static Form1 Form { set { FormController.form = value; } }

        //Controlling UserControlls
        public Control getUserControll(String name)
        {
            return form.ControllContainerPanel.Controls[name];
        }

        
        public void hideAllUserControllers()
        {
            foreach (Control control in form.ControllContainerPanel.Controls)
                control.Hide();
        }

    }
}