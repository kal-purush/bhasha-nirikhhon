using System.Reflection.Metadata.Ecma335;
using DocumentFormat.OpenXml.Drawing.Charts;
using DocumentFormat.OpenXml.Drawing.Diagrams;
using OpenQA.Selenium;
using OpenQA.Selenium.Support.UI;
using RazorEngine;
using Reqnroll;
using SeleniumExtras.WaitHelpers;


namespace qa_dotnet_cucumber.Pages
{
    public class LanguagePage
    {
        private readonly IWebDriver _driver;
        private readonly WebDriverWait _wait;

        //Constructor
        public LanguagePage(IWebDriver driver)       // Inject IWebDriver directly
        {
            _driver = driver;
            _wait = new WebDriverWait(_driver, TimeSpan.FromSeconds(10));
        }

        //Locators
        private readonly By _profileTab = By.XPath("//a[normalize-space()='Profile']");
        private readonly By _languagesTab = By.XPath("//a[normalize-space()='Languages']");
        //Add
        private readonly By _addLanguagesField = By.XPath("//input[@placeholder='Add Language']");
        private readonly By _selectLanguageLevel = By.XPath("//select[@name='level']");
        private readonly By _addButton = By.XPath("//input[@value='Add']");
        private readonly By _cancelButton = By.XPath("//input[@value='Cancel']");
        private readonly By _languageTable = By.XPath("//table[@class='ui fixed table'][.//th[normalize-space(text())='Language']]");  //whole table 
        //Edit
        private readonly By _addLanguageForUpdateField = By.XPath(".//input[@type='text']");
        private readonly By _updateButton = By.XPath(".//input[@value='Update']");
        private readonly By _cancelUpdateButton = By.XPath("//span[@class='buttons-wrapper']//input[@value='Cancel']");


        //Action Methods
        public void NavigateToTheProfilePage()
        {
            //Profile 
            var profileElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_profileTab));
            profileElement.Click();

            //Languages
            var languagesElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_languagesTab));
            languagesElement.Click();
        }

        public void AddNewLanguageAndLevel(string language, string languageLevel) //To Add new language and it's level
        {
            //Add New Languages
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var addNewElement = languageTable.FindElement(By.XPath(".//div[@class='ui teal button ' and normalize-space(text())='Add New']"));
            addNewElement.Click();

            //Enter Language
            var addLanguagesElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_addLanguagesField));
            addLanguagesElement.SendKeys(language);

            //Select Language Level
            var selectLanguageLevelDropDown = _wait.Until(ExpectedConditions.ElementToBeClickable(_selectLanguageLevel));

            SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
            selectElement.SelectByText(languageLevel);
            ClickAddButton();
        }

        public void EnterNewLanguageAndLevelToAdd(string language, string languageLevel)
        {
            //Add New Languages
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var addNewElement = languageTable.FindElement(By.XPath(".//div[@class='ui teal button ' and normalize-space(text())='Add New']"));
            addNewElement.Click();

            //Enter Language
            var addLanguagesElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_addLanguagesField));
            addLanguagesElement.SendKeys(language);

            //Select Language Level
            var selectLanguageLevelDropDown = _wait.Until(ExpectedConditions.ElementToBeClickable(_selectLanguageLevel));

            SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
            selectElement.SelectByText(languageLevel);
        }

        private void ClickAddButton()   //Click Add Button
        {
            var addButton = _wait.Until(ExpectedConditions.ElementToBeClickable(_addButton));
            addButton.Click();
        }

        public void ClickCancelButton()  //Click Cancel Button
        {
            var cancelButtonElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_cancelButton));
            cancelButtonElement.Click();
        }

        public void UpdateLanguageAndLevel(string existingLanguage, string languageToUpdate, string levelToUpdate) //To update language and it's level
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{existingLanguage}']]"));
                var editIcon = row.FindElement(By.XPath(".//i[@class='outline write icon']"));
                editIcon.Click();

                var editableRow = languageTable.FindElement(By.XPath($".//tr[.//input[@type='text' and @value='{existingLanguage}']]"));
                //Update the language
                var addLanguageForUpdateElement = editableRow.FindElement(_addLanguageForUpdateField);
                addLanguageForUpdateElement.Clear();
                addLanguageForUpdateElement.SendKeys(languageToUpdate);

                //Update Language Level
                var selectLanguageLevelDropDown = editableRow.FindElement(By.XPath(".//select[@name='level']"));

                SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
                selectElement.SelectByText(levelToUpdate);

                editableRow.FindElement(_updateButton).Click();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public void EnterLanguageAndLevelToUpdate(string existingLanguage, string languageToUpdate, string levelToUpdate) //To update language and it's level
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{existingLanguage}']]"));
                var editIcon = row.FindElement(By.XPath(".//i[@class='outline write icon']"));
                editIcon.Click();

                var editableRow = languageTable.FindElement(By.XPath($".//tr[.//input[@type='text' and @value='{existingLanguage}']]"));
                //Update the language
                var addLanguageForUpdateElement = editableRow.FindElement(_addLanguageForUpdateField);
                addLanguageForUpdateElement.Clear();
                addLanguageForUpdateElement.SendKeys(languageToUpdate);

                //Update Language Level
                var selectLanguageLevelDropDown = editableRow.FindElement(By.XPath(".//select[@name='level']"));

                SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
                selectElement.SelectByText(levelToUpdate);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public void UpdateLanguageAndLevelWithSameValue(string existingLanguage, string languageToUpdate) //To update language and level with same value
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{existingLanguage}']]"));
                var editIcon = row.FindElement(By.XPath(".//i[@class='outline write icon']"));
                editIcon.Click();

                var editableRow = languageTable.FindElement(By.XPath($".//tr[.//input[@type='text' and @value='{existingLanguage}']]"));
                //Update the language
                var addLanguageForUpdateElement = editableRow.FindElement(_addLanguageForUpdateField);
                addLanguageForUpdateElement.Clear();
                addLanguageForUpdateElement.SendKeys(languageToUpdate);

                editableRow.FindElement(_updateButton).Click();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public void ClickCancelUpdate()  //CancelUpdate
        {
            var clickCancelUpdate = _wait.Until(ExpectedConditions.ElementToBeClickable(_cancelUpdateButton));
            clickCancelUpdate.Click(); ;
        }

        public void DeleteSpecificLanguage(string languageToBeDeleted)    //To delete the specific language
        {
            if (languageToBeDeleted == null || !languageToBeDeleted.Any()) //To avoid object reference null exception
                throw new ArgumentException("Language list is empty or null");

            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{languageToBeDeleted}']]"));
            var deleteIconElement = row.FindElement(By.XPath(".//i[@class='remove icon']"));
            deleteIconElement.Click();
        }

        public void DeleteLanguages(List<string> languages)
        {
            foreach (var language in languages)
            {
                DeleteSpecificLanguage(language);
            }
        }

        public void DeleteAllLanguages()    //Delete all the languages
        {
            while (true)
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));

                var originalWait = _driver.Manage().Timeouts().ImplicitWait; // Reset the wait to 2 seconds because it waits for 10 seconds for the last iteration 
                ResetWaitTo(2);
                var deleteElements = languageTable.FindElements(By.XPath(".//i[@class='remove icon']"));
                ResetWaitTo(originalWait.Seconds);

                if (deleteElements.Count > 0)
                {
                    deleteElements[0].Click();  //delete the first element
                    Thread.Sleep(500);
                }
                else
                {
                    break;
                }
            }
        }

        private void ResetWaitTo(int seconds)
        {
            _driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(seconds);
        }

        public void LeaveTheLanguageFieldEmptyForAdd()    //To leave the language field empty for adding languages
        {
            //Click Add New Button
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var addNewElement = languageTable.FindElement(By.XPath(".//div[@class='ui teal button ' and normalize-space(text())='Add New']"));
            addNewElement.Click();

            ////Leave Language field empty
            //var addLanguagesElement = _wait.Until(ExpectedConditions.ElementToBeClickable(AddLanguagesField));
            //addLanguagesElement.SendKeys("");

            //Select Language Level
            var selectLanguageLevelDropDown = _wait.Until(ExpectedConditions.ElementToBeClickable(_selectLanguageLevel));

            SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
            selectElement.SelectByText("Basic");
            ClickAddButton();
        }

        public void NotChoosingLanguageLevelForAdd()   //Not choosing the language level for adding languages
        {
            //Click Add New Button
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var addNewElement = languageTable.FindElement(By.XPath(".//div[@class='ui teal button ' and normalize-space(text())='Add New']"));
            addNewElement.Click();

            //Leave Language field empty
            var addLanguagesElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_addLanguagesField));
            addLanguagesElement.SendKeys("Spanish");

            //Select Language Level
            var selectLanguageLevelDropDown = _wait.Until(ExpectedConditions.ElementToBeClickable(_selectLanguageLevel));

            SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
            selectElement.SelectByText("Choose Language Level");
            //  selectElement.SelectByValue("");

            ClickAddButton();
        }

        public void LeaveTheLanguageFieldEmptyAndNotChoosingLanguageLevelForAdd()   //Not selecting both language and level for adding languages
        {
            //Click Add New Button
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var addNewElement = languageTable.FindElement(By.XPath(".//div[@class='ui teal button ' and normalize-space(text())='Add New']"));
            addNewElement.Click();

            //Leave Language field empty
            var addLanguagesElement = _wait.Until(ExpectedConditions.ElementToBeClickable(_addLanguagesField));
            addLanguagesElement.SendKeys(Keys.Control + "a" + Keys.Delete);

            //Select Language Level
            var selectLanguageLevelDropDown = _wait.Until(ExpectedConditions.ElementToBeClickable(_selectLanguageLevel));

            SelectElement selectElement = new SelectElement(selectLanguageLevelDropDown);
            // selectElement.SelectByText("Choose Language Level");           
            selectElement.SelectByValue("");

            ClickAddButton();
        }

        public void LeaveTheLanguageFieldEmptyForUpdate(string existingLanguage)    //To leave the language field empty for updating languages
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{existingLanguage}']]"));
                var editIcon = row.FindElement(By.XPath(".//i[@class='outline write icon']"));
                editIcon.Click();

                var editableRow = languageTable.FindElement(By.XPath($".//tr[.//input[@type='text' and @value='{existingLanguage}']]"));
                //Update the language
                var addLanguageForUpdateElement = editableRow.FindElement(_addLanguageForUpdateField);
                addLanguageForUpdateElement.SendKeys(Keys.Control + "a" + Keys.Delete);

                //Update Language Level
                var selectLanguageLevel = editableRow.FindElement(By.XPath(".//select[@name='level']"));

                SelectElement selectLevel = new SelectElement(selectLanguageLevel);
                selectLevel.SelectByText("Fluent");
                //selectLevel.SelectByValue("");

                editableRow.FindElement(_updateButton).Click();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public void NotChoosingLanguageLevelForUpdate(string existingLanguage)   //Not choosing the language level for updating languages
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{existingLanguage}']]"));
                var editIcon = row.FindElement(By.XPath(".//i[@class='outline write icon']"));
                editIcon.Click();

                var editableRow = languageTable.FindElement(By.XPath($".//tr[.//input[@type='text' and @value='{existingLanguage}']]"));

                //Update Language Level
                var selectLanguageLevel = editableRow.FindElement(By.XPath(".//select[@name='level']"));

                SelectElement selectLevel = new SelectElement(selectLanguageLevel);
                selectLevel.SelectByText("Language Level");

                editableRow.FindElement(_updateButton).Click();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public void LeaveTheLanguageFieldEmptyAndNotChoosingLanguageLevelForUpdate(string existingLanguage)   //Not selecting both language and level for updating languages
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{existingLanguage}']]"));
                var editIcon = row.FindElement(By.XPath(".//i[@class='outline write icon']"));
                editIcon.Click();

                var editableRow = languageTable.FindElement(By.XPath($".//tr[.//input[@type='text' and @value='{existingLanguage}']]"));
                //Update the language
                var addLanguageForUpdateElement = editableRow.FindElement(_addLanguageForUpdateField);
                addLanguageForUpdateElement.SendKeys("");

                //Update Language Level
                var selectLanguageLevel = editableRow.FindElement(By.XPath(".//select[@name='level']"));

                SelectElement selectLevel = new SelectElement(selectLanguageLevel);
                selectLevel.SelectByText("Language Level");

                editableRow.FindElement(_updateButton).Click();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public string GetSuccessMessageForAddNew(string languageToBeAdded) //To get the success message for add new languages for validation
        {
            try
            {
                var successMessage = _wait.Until(ExpectedConditions.ElementIsVisible(By.XPath($"//div[@class='ns-box-inner' and  contains(text(), '{languageToBeAdded} has been added to your languages')]")));
                return successMessage.Text;
            }
            catch
            {
                return string.Empty;
            }
        }

        public List<string> GetAllAddedLanguages()  //To get the languages list after adding for validation
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var addedLanguages = new List<string>();
                var rows = languageTable.FindElements(By.XPath(".//tbody/tr"));
                foreach (var row in rows)
                {
                    var languageCell = row.FindElement(By.XPath("./td[1]"));
                    addedLanguages.Add(languageCell.Text.Trim());
                }
                return addedLanguages;
            }
            catch
            {
                return new List<string>();
            }
        }

        public string GetLevelOfLanguage(string language)
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{language}']]"));
                var levelCell = row.FindElement(By.XPath("./td[2]"));
                return levelCell.Text.Trim();
            }
            catch
            {
                return string.Empty;
            }
        }

        public List<AddLanguage> GetAddedLanguagesAndLevel()
        {
            var actualLanguagesAndLevel = new List<AddLanguage>();
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var rows = languageTable.FindElements(By.XPath(".//tbody/tr"));
            foreach (var row in rows)
            {
                var cells = row.FindElements(By.TagName("td"));
               if (cells.Count >= 2)
                {
                    var languageName = cells[0].Text.Trim();
                    var languageLevel = cells[1].Text.Trim();
                    actualLanguagesAndLevel.Add(new AddLanguage
                    {
                        NewLanguage = languageName,
                        NewLanguageLevel = languageLevel
                    });
                }
            }

            return actualLanguagesAndLevel;
        }

       public string GetSuccessMessageForUpdate(string languageToBeUpdated) //To get success message for update for validation
        {
            try
            {
                var successMessageForUpdate = _wait.Until(ExpectedConditions.ElementIsVisible(By.XPath($"//div[@class='ns-box-inner' and  contains(text(), '{languageToBeUpdated} has been updated to your languages')]")));
                return successMessageForUpdate.Text;
            }
            catch
            {
                return string.Empty;
            }
        }

        public List<string> GetAllUpdatedLanguages() //To get the languages list after updating for validation
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var addedLanguages = new List<string>();
                var rows = languageTable.FindElements(By.XPath(".//tbody/tr"));
                foreach (var row in rows)
                {
                    var languageCell = row.FindElement(By.XPath("./td[1]"));
                    addedLanguages.Add(languageCell.Text.Trim());
                }
                return addedLanguages;
            }
            catch
            {
                return new List<string>();
            }
        }

        public string GetSuccessMessageForDelete(string languageToBeDeleted) //To get the success message after deleting
        {
            Thread.Sleep(5000);
            var successMessageForDelete = _wait.Until(ExpectedConditions.ElementIsVisible(By.XPath($"//div[@class='ns-box-inner' and  contains(text(), '{languageToBeDeleted} has been deleted from your languages')]")));
            return successMessageForDelete.Text;
        }

        public bool IsLanguageTableEmpty()  //To check the table is empty after deleting all languages for validation
        {
            var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
            var rows = languageTable.FindElements(By.XPath(".//tbody/tr"));
            return rows.Count == 0;
        }

        public bool IsErrorMessageDisplayed(string error)  //Error Message for both and any of the fields empty
        {
            try
            {
                var popUpMessageElement = _wait.Until(ExpectedConditions.ElementIsVisible(By.XPath($"//div[contains(@class, 'ns-type-error') and contains(@class, 'ns-show')]/div[@class='ns-box-inner' and contains(text(), '{error}')]")));
                return true;// Found the Error message
            }

            catch (Exception ex)
            {
                Console.WriteLine($"Error message not found: {ex.Message}");
                return false;
            }
        }

        public bool IsCancelButtonNotDisplayed()  //Check the visibility of cancel button 
        {
            try
            {
                var cancelButtonNotVisible = _wait.Until(ExpectedConditions.InvisibilityOfElementLocated(_cancelButton));
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void ExpireSession() //To delete the token to get the session timeout message
        {
            try
            {
                _driver.Manage().Cookies.DeleteCookieNamed("marsAuthToken");
            }
            catch
            {

            }
        }

        public bool IsLanguageNotAdded(string language) // This function should return true when the language is not added else should return false; 
        {
            try
            {
                var languageTable = _wait.Until(ExpectedConditions.ElementIsVisible(_languageTable));
                var row = languageTable.FindElement(By.XPath($".//tr[td[normalize-space(text())='{language}']]"));
                return false;
            }
            catch
            {
                return true;
            }

        }
    }

}

