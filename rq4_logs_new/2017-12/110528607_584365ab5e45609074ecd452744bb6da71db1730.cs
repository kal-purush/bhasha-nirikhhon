using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CRM_GTMK.Control;

namespace CRM_GTMK.Visual
{
    class MyVisual
    {
	    private Controller _controller;
		
		public MyVisual(Controller controller)
		{
			_controller = controller;
            SetUpStartWindow();
            ShowTheStartWindow();
        }
        //TODO открыть стартовое окно
        private void SetUpStartWindow()
        {
            //TODO set the properties of the main window
        }

        private void ShowTheStartWindow()
        {
	        

		}
    }
}