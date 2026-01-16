using install.Basic;
using install.Basic.ModelsParts;
using install.Basic.ModelsParts.Types.ResultTypes;
using install.Interfaces.Installer;
using install.WorkWithDataBase.MsSqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace install.Installer
{
    class GeneralInstallator
    {
        private InstallerInterface installer;

        public GeneralInstallator(InstallerInterface installer)
        {
            this.installer = installer;
        }

        public ModelsState install(ModelsState currentState)
        {
            if (dataBaseReady(currentState))
            {
                if (currentState.config.programType.getType().Equals("AnalitycsType"))
                {
                    try
                    {
                        installer.installAnalytics(currentState.config.programPath,
                            currentState.config.connection);
                        currentState.result = new Result(new OkType());
                        return currentState;
                    }
                    catch (Exception ex)
                    {
                        ExceptionHandler.Concrete.ExceptionHandler.getInstance().processing(ex);
                        currentState.result = new Result(new CancelType());
                        return currentState;
                    }
                }
                else
                {
                    try
                    {
                        Director installDirector = new Director(new ConcreteIntallBuilder());
                        installDirector.install(currentState.config.copy());
                        currentState.result = new Result(new OkType());
                        return currentState;
                    }
                    catch (Exception ex)
                    {
                        ExceptionHandler.Concrete.ExceptionHandler.getInstance().processing(ex);
                        currentState.result = new Result(new CancelType());
                        return currentState;
                    }
                }
            }
            else
            {
                currentState.result = new Result(new DataBaseHaventTablesType());
                return currentState;
            }
        }

        private bool dataBaseReady(ModelsState currentState)
        {
            //Проверка, доступна ли таблицы БД. Если доступны, то ранее уже 
            //создавалась структура БД и есть минимум один администратор
            //(копия этой программы установлена на другом компьютере и уже все сделала)
            //Если нет, то нужно создать структуру таблиц и администратора
            List<string> checkTables = new List<string>();
            checkTables.Add("SELECT 1 FROM Users");
            checkTables.Add("SELECT 1 FROM Vendor");
            checkTables.Add("SELECT 1 FROM Software");
            checkTables.Add("SELECT 1 FROM History");
            checkTables.Add("SELECT 1 FROM UST");

            bool ready = true;
            MsSqlServerController msSqlServerController = new MsSqlServerController();
            for (int i = 0; i < checkTables.Count; i++)
            {
                if (!msSqlServerController.configAndExecute(currentState.config.connection,
                "SELECT 1 FROM UST"))
                {
                    ready = false;
                    break;
                }
            }
            return ready;
        }
    }
}