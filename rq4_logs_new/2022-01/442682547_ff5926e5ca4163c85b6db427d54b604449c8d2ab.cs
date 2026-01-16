namespace BeeFor.Domain.ValuesObjects.Response
{
    public class ColunaNomeReponse
    {
        public string Self { get; set; }
        public string Description { get; set; }
        public string IconUrl { get; set; }
        public string Name { get; set; }
        public string UntranslatedName { get; set; }
        public string Id { get; set; }
        public _StatusCategory StatusCategory { get; set; }
        public _Scope Scope { get; set; }
    }
    public class _StatusCategory
    {
        public string Self { get; set; }
        public int Id { get; set; }
        public string Key { get; set; }
        public string ColorName { get; set; }
        public string Name { get; set; }
    }

    public class _Project
    {
        public string Id { get; set; }
    }

    public class _Scope
    {
        public string Type { get; set; }
        public _Project Project { get; set; }
    }

}