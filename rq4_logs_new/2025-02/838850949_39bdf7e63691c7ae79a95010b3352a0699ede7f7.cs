namespace ModelLibrary.Dto
{
    public class CartUpsertDto
    {
        public string? UserId { get; set; }
        public int ProductId { get; set; }
        public int Count { get; set; }
    }
}