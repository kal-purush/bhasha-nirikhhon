namespace SoftmakeAllSDK.Fluent
{
  public abstract class BaseEntity
  {
    #region Constructor
    public BaseEntity() { }
    #endregion

    #region Properties
    public System.String ID { get; set; }
    public System.Guid UniqueID { get; set; }
    public System.String RowVersion { get; set; }
    public System.DateTimeOffset Inserted { get; set; }
    public System.Nullable<System.DateTimeOffset> Updated { get; set; }
    public System.Text.Json.JsonElement JSONData { get; set; }
    #endregion
  }
}