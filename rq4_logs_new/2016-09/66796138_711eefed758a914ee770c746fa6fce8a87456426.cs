using BoDi;

namespace WebSpecs.Support
{
    public abstract class Base
    {
        protected readonly IObjectContainer objectContainer;
        protected readonly PageBrowserSession browser;
        protected Page Page { get { return objectContainer.Resolve<Page>(); } }

        protected Base(IObjectContainer objectContainer)
        {
            this.objectContainer = objectContainer;
            browser = objectContainer.Resolve<PageBrowserSession>();
        }

        protected void PageFor(string pageName)
        {
            var page = PageFactory.Instance.Create(pageName, browser);
            objectContainer.RegisterInstanceAs(page, typeof(Page));
        }
    }
}