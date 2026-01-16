using System.Collections.Generic;
using FA1.Src.Common;

namespace FA1.Src.Entities;

public sealed class AdditionalUserInfoEntity : BaseEntity<long>
{
    public string FirstName { get; set; }

    public string LastName { get; set; }

    public string Description { get; set; }

    #region Navigations
    public IEnumerable<TodoTaskListEntity> TodoTaskLists { get; set; }
    #endregion

    public static class Metadata
    {
        public static readonly string TableName = "additional_user_info";

        public static class Properties
        {
            public static class FirstName
            {
                public const string ColumnName = "first_name";

                public const bool IsNotNull = true;

                public const short MaxLength = 255;
            }

            public static class LastName
            {
                public const string ColumnName = "last_name";

                public const bool IsNotNull = true;

                public const short MaxLength = 255;
            }

            public static class Description
            {
                public const string ColumnName = "description";

                public const bool IsNotNull = true;

                public const ushort MaxLength = ushort.MaxValue;
            }
        };
    }
}