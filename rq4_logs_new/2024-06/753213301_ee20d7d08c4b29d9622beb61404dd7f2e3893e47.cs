namespace amazon_backend.Data.Model
{
    public class UpdateCategoryModel
    {
        public uint Id { get; set; }
        public int? ParentCategoryId { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string ImageId { get; set; }
        public bool IsActive { get; set; }
        public string Logo { get; set; }
        public List<CategoryPropertyKeyModel> PropertyKeys { get; set; }
    }
}
using MediatR;
        public uint Id { set; get; }   