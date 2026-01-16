using Portfolio.Data.Entities;

namespace Portfolio.Core.ViewModels
{
    public class TagViewModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Type { get; set; } = "";
    }

    public static class TagViewModelMapper
    {
        public static TagViewModel ToViewModel(this Tag tag)
        {
            return new TagViewModel()
            {
                Id = tag.Id,
                Name = tag.Name,
                Type = tag.Type.HasValue ? Enum.GetName(typeof(TagType), tag.Type.Value) ?? "" : ""
            };
        }
    }

}