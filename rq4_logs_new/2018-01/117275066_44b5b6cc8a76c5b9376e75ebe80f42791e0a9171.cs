using Domain;
using FluentNHibernate.Automapping;
using FluentNHibernate.Cfg;
using FluentNHibernate.Cfg.Db;
using NHibernate;
using NHibernate.Tool.hbm2ddl;

public class NHibernateHelper
{
    public static ISession OpenSession<T>() where T: IEntity
    {
        ISessionFactory sessionFactory = Fluently.Configure()
            //Настройки БД. Надо бы по хорошему сделать нормально и прописать в конфиге, но мне лень
            .Database(MsSqlConfiguration.MsSql2008.ConnectionString(@"Data Source=DESKTOP-51T48C5\SQLEXPRESS;Initial Catalog=SUBD;Integrated Security=True;")
                .ShowSql()
            )
            //Маппинг. Используя AddFromAssemblyOf NHibernate будет пытаться маппить КАЖДЫЙ класс в этой сборке (assembly). Можно выбрать любой класс. 
            .Mappings(m => m.FluentMappings.AddFromAssemblyOf<T>())
            //SchemeUpdate позволяет создавать/обновлять в БД таблицы и поля (2 поле ==true) 
            .ExposeConfiguration(cfg => new SchemaUpdate(cfg).Execute(false, true))
            .BuildSessionFactory();
        return sessionFactory.OpenSession();
    }

    public static class FluentNHibernateHelper

    {

        public static ISession OpenSession()

        {

            string connectionString = @"Data Source=DESKTOP-51T48C5\SQLEXPRESS;Initial Catalog=SUBD;Integrated Security=True;";

            ISessionFactory sessionFactory = Fluently.Configure()

                .Database(MsSqlConfiguration.MsSql2012

                    .ConnectionString(connectionString).ShowSql()

                )

                .Mappings(m =>

                    m.FluentMappings

                        .AddFromAssemblyOf<Server>())

                .ExposeConfiguration(cfg => new SchemaExport(cfg)

                    .Create(false, false))

                .BuildSessionFactory();

            return sessionFactory.OpenSession();

        }

    }

    private static ISessionFactory BuildSessionFactory()
        
    {
        AutoPersistenceModel model = CreateMappings();
       
 
        return Fluently.Configure()
        
            .Database(MsSqlConfiguration.MsSql2005
       
            .ConnectionString(c => c
        
            .Server("MYCOMPUTER\\SQLEXPRESS")
        
            .Database("testdb")
        
            .Username("test")
        
            .Password("test")))
        
            .Mappings(m => m
        
            .AutoMappings.Add(model))
        
            .ExposeConfiguration(BuildSchema)
        
            .BuildSessionFactory();
        
    }

    private static AutoPersistenceModel CreateMappings()
    {
        return AutoMap
            .Assembly(System.Reflection.Assembly.GetCallingAssembly())
            .Where(t => t.Namespace == "SimpleOrmApplication.Model");
    }

}