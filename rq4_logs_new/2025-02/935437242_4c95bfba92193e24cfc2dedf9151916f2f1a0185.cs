namespace PortfolioProject.Models
{
    /// <summary>
    /// Used to send a set of objects to 
    /// webpages for display
    /// </summary>
    public class ViewPhotosViewModel
    {
        // Contains a list of Section objects to 
        // be sent to a view, these will be 
        // organized in the SectionsController
        public List<Section>? Sections { get; set; }

        // Contains a list of Bookmark objects to 
        // be sent to a view, these will be 
        // organized in the SectionsController
        public List<Bookmark>? Bookmarks { get; set; }
    }
}