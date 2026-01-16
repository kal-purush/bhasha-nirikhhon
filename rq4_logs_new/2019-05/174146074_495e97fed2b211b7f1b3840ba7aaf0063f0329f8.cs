    using OpenQA.Selenium;

    namespace DEV_9.PageObjects.MailRu
    {
        /// <summary>
        /// Main mail page.
        /// </summary>
        public class MailStartPage
        {
            /// <summary>
            /// The WebDriver.
            /// </summary>
            private IWebDriver driver;

            /// <summary>
            /// The button for writing a new letter.
            /// </summary>
            public IWebElement WriteNewLetterButton => this.driver.FindElement(By.XPath("//a[@data-name='compose']"));
            
            /// <summary>
            /// Initializes a new instance of the <see cref="MailStartPage"/> class.
            /// </summary>
            /// <param name="driver">
            /// The WebDriver.
            /// </param>
            public MailStartPage(IWebDriver driver)
            {
                this.driver = driver;
            }

            /// <summary>
            /// Proceed to the writing of a new letter.
            /// </summary>
            /// <returns>
            /// The <see cref="WriteNewLetterPage"/>.
            /// </returns>
            public WriteNewLetterPage WriteNewLetter()
            {
                this.WriteNewLetterButton.Click();
                return new WriteNewLetterPage(this.driver);
            }
        }
    }