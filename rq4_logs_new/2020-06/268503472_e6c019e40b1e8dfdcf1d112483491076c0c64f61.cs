using System.Collections.Generic;

namespace DocumentFactory
{
    public class ApplicationFactory : DocumentFactory
    {
        public override IDocument CreateDocument()
        {
            var pageNumber = 1;
            var application = new Application()
            {
                Title = "Application",
                Pages = new List<Page>
                {
                    CreatePage("Introduction", pageNumber++),
                    CreatePage("Development", pageNumber++),
                    CreatePage("Conclusion", pageNumber),
                }
            };

            return application;
        }
    }
}