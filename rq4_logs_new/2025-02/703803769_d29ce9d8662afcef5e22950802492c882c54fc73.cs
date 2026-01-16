using LegalLead.PublicData.Search.Classes;

namespace LegalLead.PublicData.Search
{
    public class PreviewSearchRequestedEvent : WebsiteChangeEvent
    {
        public override string Name => @"Preview";
        public override void Change()
        {
            var main = GetMain;
            var defaultDef = new DefaultStyleCollection(main);
            defaultDef.HideRows();
            var progres = main.Controls.Find("ButtonDentonSetting", true);
            if (progres == null || progres.Length == 0) return;
            progres[0].Visible = false;
        }
    }
}