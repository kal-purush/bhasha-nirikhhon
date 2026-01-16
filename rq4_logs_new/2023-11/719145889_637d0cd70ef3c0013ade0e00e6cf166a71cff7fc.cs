// FindApplicantByWordMatch
using legallead.records.search.Models;
using OpenQA.Selenium;

namespace legallead.records.search.Addressing
{
    public class FindApplicantByWordMatch : FindDefendantBase
    {
        public override bool CanFind { get; set; }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types",
            Justification = "Exception thrown from this method will stop automation.")]
        public override void Find(IWebDriver driver, HLinkDataRow linkData)
        {
            if (driver == null)
            {
                throw new ArgumentNullException(nameof(driver));
            }

            if (linkData == null)
            {
                throw new ArgumentNullException(nameof(linkData));
            }

            var searchType = IndexKeyNames.Applicant;
            CanFind = false;
            // IndexKeyNames.ThContainsText
            var tdName = TryFindElement(driver, By.XPath(
                string.Format(CultureInfo.CurrentCulture, IndexKeyNames.ThContainsText, searchType)));
            // this instance can find
            if (tdName == null)
            {
                return;
            }

            var parent = tdName.FindElement(By.XPath(IndexKeyNames.ParentElement));
            var rowLabel = parent.FindElements(By.TagName(IndexKeyNames.ThElement))[1];
            linkData.Defendant = rowLabel.GetAttribute(IndexKeyNames.InnerText);
            CanFind = true;
            linkData.Address = parent.Text;
            try
            {
                // get row index of this element ... and then go one row beyond...
                var ridx = parent.GetAttribute(IndexKeyNames.RowIndex);
                var table = parent.FindElement(By.XPath(IndexKeyNames.ParentElement));
                var trCol = table.FindElements(By.TagName(IndexKeyNames.TrElement)).ToList();
                if (!int.TryParse(ridx, out int r))
                {
                    return;
                }

                MapElementAddress(linkData, rowLabel, table, trCol, r,
                    searchType.ToLower(CultureInfo.CurrentCulture));
            }
#pragma warning disable CA1031 // Do not catch general exception types
            catch (Exception)
#pragma warning restore CA1031 // Do not catch general exception types
            {
            }
        }
    }
}