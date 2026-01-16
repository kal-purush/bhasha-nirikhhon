using install.Basic.Model.Chain;
using install.Basic.ModelsParts.Types.ResultTypes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace install.Basic.ModelsParts.Chain.Concrete
{
    class AvevaConfig : BaseConfigReader
    {
        public override Config read(ModelsState modelsState, Config newConfig)
        {
            if (newConfig.avevasPath != null)
            {
                if (Directory.Exists(newConfig.avevasPath))
                {
                    modelsState.config.avevasPath = newConfig.avevasPath;
                    modelsState.result = new Result(new OkType());
                    return modelsState.config;
                }
                else
                {
                    showErrorMessage("Ошибка: указанный путь не существует.");
                    modelsState.result = new Result(new CancelType());
                    return modelsState.config;
                }
            }
            else
            {
                return giveNext(modelsState, newConfig);
            }
        }

        private void showErrorMessage(string error)
        {
            string message = error;
            string caption = "Ошибка";
            DialogResult result;
            result = MessageBox.Show(message, caption);
        }
    }
}