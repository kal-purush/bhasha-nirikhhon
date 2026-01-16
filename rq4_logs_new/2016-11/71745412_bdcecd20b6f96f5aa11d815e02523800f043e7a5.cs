using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;     

namespace GameSlam.Core.Enums
{
    public class SystemType
    {
        private SystemType(SystemTypeEnum @enum)
        {
            Id = (int)@enum;
            Name = @enum.ToString();
            Description = Extentions.EnumExtention.GetDescription(@enum);
        }

        protected SystemType() { } //For EF

        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        [Required, MaxLength(100)]
        public string Name { get; set; }

        [MaxLength(100)]
        public string Description { get; set; }

        public static implicit operator SystemType(SystemTypeEnum @enum) => new SystemType(@enum);

        public static implicit operator SystemTypeEnum(SystemType faculty) => (SystemTypeEnum)faculty.Id;
    }

    public class Category
    {
        private Category(CategoryEnum @enum)
        {
            Id = (int)@enum;
            Name = @enum.ToString();
            Description = Extentions.EnumExtention.GetDescription(@enum);
        }

        protected Category() { } //For EF

        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        [Required, MaxLength(100)]
        public string Name { get; set; }

        [MaxLength(100)]
        public string Description { get; set; }

        public static implicit operator Category(CategoryEnum @enum) => new Category(@enum);

        public static implicit operator CategoryEnum(Category category) => (CategoryEnum)category.Id;
    }


    public class ApprovalStatus
    {
        private ApprovalStatus(ApprovalStatusEnum @enum)
        {
            Id = (int)@enum;
            Name = @enum.ToString();
            Description = Extentions.EnumExtention.GetDescription(@enum);
        }

        protected ApprovalStatus() { } //For EF

        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        [Required, MaxLength(100)]
        public string Name { get; set; }

        [MaxLength(100)]
        public string Description { get; set; }

        public static implicit operator ApprovalStatus(ApprovalStatusEnum @enum) => new ApprovalStatus(@enum);

        public static implicit operator ApprovalStatusEnum(ApprovalStatus category) => (ApprovalStatusEnum)category.Id;
    }
}