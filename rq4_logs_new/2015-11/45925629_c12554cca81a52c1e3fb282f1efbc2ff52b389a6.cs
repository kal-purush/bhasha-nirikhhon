using System;
using System.Collections.Generic;
using System.Linq;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.AutoMoq;
using Moq;
using System.Linq.Expressions;

//=================
//MAKE SURE TO ADD TO INCLUDE USING STATEMENTS FOR YOUR REPOSITORIES
//EXAMPLE:
//using MyProject.Repositories
//================

namespace Tests.Utilities

{
    /*
        Class: Repo
        Written by: Aaron Gates
        Date: September 11, 2015

        This class abstracts the process of making fake repositories
        used for unit testing using AutoFixture and Moq.

        CONSTRUCTOR:
        Repo<M>(ignoreCirlces?, howMany?) = constructor, M is the database model you want to fake in the repository
                                            @ignoreCircles (optional) : whether or not to ignore circular
                                                                        references in the database models,
                                                                        defaults to false
                                            @howMany (optional) : how many completely random fake records
                                                                  to make to fill out the repository,
                                                                  defaults to 3

        CHAINABLE METHODS:
        .Add(property, value) = adds a specific record to the repository. 
                                @property : name of the property of the model as a string. 
                                @value : what you want the property to be [can be any type]
                                **(property1, value1, property2, value2, etc) up to 5 will add more than one property to the same record

        .Empty(howMany?) = removes all records from the repository
                           @howMany (optional) : same as in the constructor, but defaults to 0

        FINISHING METHODS (MUST CALL AFTER ADDS TO MAKE REPOSITORY USABLE): 
        .C1() = completes and returns a Context1 context repository

        .C2() = completes and returns a Context2 context repository

        .C3() = completes and returns a Context3 context repository
    */

  //============================
  //
  //  REPLACE ALL 'CONTEXT1','CONTEXT2','CONTEXT3' WITH YOUR OWN DATABASE CONTEXTS
  //
  //============================


    class Repo<M> where M : class
    {
        private IFixture fixture;
        public List<M> records;

        //the contexts=====IF YOU HAVE MULTIPLE DATABASE CONTEXTS, ADD ENUMERABLE CONSTANTS HERE, OTHERWISE IGNORE
        private const int CONTEXT1 = (int)Enums.DBContexts.Context1;
        private const int CONTEXT2 = (int)Enums.DBContexts.Context2;
        private const int CONTEXT3 = (int)Enums.DBContexts.Context3;

        //the mock repos
        private Mock<IBaseRepositoryContext1<M>> Context1Repo;
        private Mock<IBaseRepositoryContext2<M>> Context2Repo;
        private Mock<IBaseRepositoryContext3<M>> Context3Repo;

        //the money makers
        public IBaseRepositoryContext1<M> C1() => this.BringToLife(CONTEXT1).Context1Repo.Object;
        public IBaseRepositoryContext2<M> C2() => this.BringToLife(CONTEXT2).Context2Repo.Object;
        public IBaseRepositoryContext3<M> C3() => this.BringToLife(CONTEXT3).Context3Repo.Object;

        //the constructor
        public Repo(bool ignoreCircles = false, int howMany = 3)
        {
            fixture = new Fixture().Customize(new AutoMoqCustomization());
            if (ignoreCircles) StopCaringAboutCircles();
            records = fixture.CreateMany<M>(howMany).ToList();
        }
           
        //bring the fake repo to life -- depends on which DB context
        private Repo<M> BringToLife(int context)
        {
            switch (context)
            {
                case CONTEXT1:
                    Context1Repo = fixture.Freeze<Mock<IBaseRepositoryContext1Repo<M>>>();
                    Context1Repo.Setup(u => u.Get()).Returns(records.AsQueryable);
                    break;
                case CONTEXT2:
                    Context2Repo = fixture.Freeze<Mock<IBaseRepositoryContext2Repo<M>>>();
                    Context2Repo.Setup(u => u.Get()).Returns(records.AsQueryable);
                    break;
                case CONTEXT3:
                    Context3Repo = fixture.Freeze<Mock<IBaseRepositoryContext3Repo<M>>>();
                    Context3Repo.Setup(u => u.Get()).Returns(records.AsQueryable);
                    break;
                default:
                    throw  new Exception("Invalid context value. Must be 0-2.");
            }
            return this;
        }
        
        
        
        //------OVERLOADED ADD FUNCTIONS FOR ADDING SPECIFIC PROPERTIES TO RECORDS----//
        public Repo<M> Add<V>(string prop1, V val1)
        {
            records.Add(fixture.Build<M>()
                .With(MakeExpression(prop1, val1), val1)
                .Create());
            return this;
        }

        public Repo<M> Add<V1,V2>(string prop1, V1 val1, string prop2, V2 val2)
        {
            records.Add(fixture.Build<M>()
                .With(MakeExpression(prop1, val1), val1)
                .With(MakeExpression(prop2, val2), val2)
                .Create());
            return this;
        }

        public Repo<M> Add<V1, V2, V3>(string prop1, V1 val1, string prop2, V2 val2, string prop3, V3 val3)
        {
            records.Add(fixture.Build<M>()
                .With(MakeExpression(prop1, val1), val1)
                .With(MakeExpression(prop2, val2), val2)
                .With(MakeExpression(prop3, val3), val3)
                .Create());
            return this;
        }

        public Repo<M> Add<V1, V2, V3, V4>(string prop1, V1 val1, string prop2, V2 val2, string prop3, V3 val3, string prop4, V4 val4)
        {
            records.Add(fixture.Build<M>()
                .With(MakeExpression(prop1, val1), val1)
                .With(MakeExpression(prop2, val2), val2)
                .With(MakeExpression(prop3, val3), val3)
                .With(MakeExpression(prop4, val4), val4)
                .Create());
            return this;
        }

        public Repo<M> Add<V1, V2, V3, V4, V5>(string prop1, V1 val1, string prop2, V2 val2, string prop3, V3 val3, string prop4, V4 val4, string prop5, V5 val5)
        {
            records.Add(fixture.Build<M>()
                .With(MakeExpression(prop1, val1), val1)
                .With(MakeExpression(prop2, val2), val2)
                .With(MakeExpression(prop3, val3), val3)
                .With(MakeExpression(prop4, val4), val4)
                .With(MakeExpression(prop5, val5), val5)
                .Create());
            return this;
        }
        //----------------END OF ADD METHODS-------------//

        public Repo<M> Empty(int howMany = 0)
        {
            this.records.Clear();
            if (howMany != 0) this.records = fixture.CreateMany<M>(howMany).ToList();
            return this;
        }

        //separated for code clarity
        private void StopCaringAboutCircles()
        {
            fixture.Behaviors.Remove(new ThrowingRecursionBehavior());
            fixture.Behaviors.Add(new OmitOnRecursionBehavior());
        } 
        
        //create Expression<Func<M, T>> for add method
        private Expression<Func<M,V>> MakeExpression<V>(string prop, V val)
        {
            var propType = typeof(M).GetProperty(prop).PropertyType;
            var valType = typeof(V);

            if (propType != valType) throw new Exception("Error in MakeExpression. " + propType + " must match " + valType);

            var entityParam = Expression.Parameter(typeof(M), "m");
            Expression columnExpr = Expression.Property(entityParam,typeof(M).GetProperty(prop));

            return Expression.Lambda<Func<M, V>>(columnExpr, entityParam);
        }
    }
}