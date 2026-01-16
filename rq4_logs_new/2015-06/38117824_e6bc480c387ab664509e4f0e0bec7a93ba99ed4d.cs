using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using FluentNHibernate.Mapping;
using NHibernateTest.Models;

namespace NHibernateTest.Mapping
{
    public class AreaMap : ClassMap<Area>
    {
        public AreaMap()
        {
            Table("T_AREA");
            Schema("dbo");
            LazyLoad();
            Id(x => x.CD_AREA)
                .GeneratedBy
                .Identity()
                .Column("CD_AREA");

            Map(x => x.DE_AREA).Column("DE_AREA").Not.Nullable();
            Map(x => x.CA_AREA).Column("CA_AREA");
            Map(x => x.FL_DELETE).Column("FL_DELETE").Not.Nullable();
        }
    }
}