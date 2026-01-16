using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace WindowsFormsApplication8
{
    class EventClickOnButtonOfSaveToDatabase
    {
        public static void buttonSaveToDatabase(Form5 form)
        {
           try
           {
                checkNameOfPartAndNameOfSurface(form);
                saveToDatabase(form);
                MessageBox.Show("Сохранение прошло успешно", "Ошибка");
                form.Close();
            }
           catch { MessageBox.Show("Ошибка Базы данных", "Ошибка"); }
        }

        private static bool checkNameOfPartAndNameOfSurface(Form5 form)
        {
            if (form.textBox1.Text.Equals(""))
            {
                MessageBox.Show("Введите наименование детали", "Ошибка");
                return false;
            }

            if (form.textBox2.Text.Equals(""))
            {
                MessageBox.Show("Введите обозначение поверхности", "Ошибка");
                return false;
            }

            return true;
        }

        private static void saveToDatabase(Form5 form)
        {
            saveToTableOfPart(form);
            int index = form.dataGridView1.NewRowIndex;
            saveWorkpieceToTableOfOperations(form, index);
            saveToTableOfOperations(form, index);
        }

        private static void saveToTableOfPart(Form5 form)
        {
            string nameOfPart = form.textBox1.Text;
            string nameOfSurface = form.textBox2.Text;

            DateTime date = DateTime.Now;
            ParametersOfPart parametersOfPart = Part.getParametersOfPart();
            ParametersWorkpiece parametersWorkpiece = Part.getWorkpiece();

            form.детальTableAdapter1.Insert(nameOfPart, date, (float)parametersOfPart.getLengthOfPart(), (float)parametersOfPart.getDiameterOfPart(), parametersWorkpiece.getKvalitet(), (float)parametersWorkpiece.getSurfaceRoughnessRz(), (float)parametersWorkpiece.getThicknessOfDefectiveCoating(), parametersOfPart.getTypeOfPart().getName(), parametersOfPart.getTypeOfProcessedSurface().getName(), parametersOfPart.getTypeOfAllowance().getName(), parametersWorkpiece.getNameOfWorkpiece(), nameOfSurface);
        }

        private static void saveToTableOfOperations(Form5 form, int index)
        {
            Surface surface = Part.getSurfaceOnIndex(0);

            ParametersOperation[] parametersOperations = surface.getOperations();
            ClassesToCalculate.ResultsOfCalculation resultsOfCalculation = surface.getResultsOfCalculation();

            int numberOfOperation = surface.getNumberOfOperations();

            for (int i = 0; i < numberOfOperation; i++)
            {
                ParametersOperation parametersOperation = parametersOperations[i];

                string nameOperation = parametersOperation.getNameOperation() + ',' + parametersOperation.getPrecisionOfMachining();

                form.переходыTableAdapter.Insert(Convert.ToInt16(form.dataGridView1[0, index - 1].Value.ToString()) + 1, i + 1, nameOperation, parametersOperation.getTypeOfInstrument(), (float)parametersOperation.getSurfaceRoughnessRz(), (float)parametersOperation.getThicknessOfDefectiveCoating(), (float)resultsOfCalculation.getSpatialDeviation()[i + 1], (float)resultsOfCalculation.getdeviationOfInstallation()[i + 1], (float)resultsOfCalculation.getAccuracies()[i + 1], (float)resultsOfCalculation.getNominalAllowance()[i + 1], (float)resultsOfCalculation.getSizeOfWorkprieceAfterOperation()[i + 1], parametersOperation.getIdOperation(), null, (float)parametersOperation.getCoefficientOfRefinement(), (int)parametersOperation.getKvalitet());
            }
        }

        private static void saveWorkpieceToTableOfOperations(Form5 form, int index)
        {
            ParametersWorkpiece parametersWorkpiece = Part.getWorkpiece();
            
            ClassesToCalculate.ResultsOfCalculation resultOfCalculation = Part.getSurfaceOnIndex(0).getResultsOfCalculation();

            form.переходыTableAdapter.Insert(Convert.ToInt16(form.dataGridView1[0, index - 1].Value.ToString()) + 1, 0, parametersWorkpiece.getNameOfWorkpiece(), "", (float)parametersWorkpiece.getSurfaceRoughnessRz(), (float)parametersWorkpiece.getThicknessOfDefectiveCoating(), (float)resultOfCalculation.getSpatialDeviation()[0], (float)resultOfCalculation.getdeviationOfInstallation()[0], (float)resultOfCalculation.getAccuracies()[0], (float)resultOfCalculation.getNominalAllowance()[0], (float)resultOfCalculation.getSizeOfWorkprieceAfterOperation()[0], parametersWorkpiece.getIdWorkpiece(), null, (float)parametersWorkpiece.getValidOffsetSurface(), (int)parametersWorkpiece.getKvalitet());
        }
    }
}