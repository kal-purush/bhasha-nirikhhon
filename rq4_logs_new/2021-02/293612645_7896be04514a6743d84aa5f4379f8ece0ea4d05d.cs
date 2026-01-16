// -------------------------------------------------------------------------------------------------
// <copyright file="ParameterStateRowViewModelTestFixture.cs" company="RHEA System S.A.">
//    Copyright (c) 2020-2020 RHEA System S.A.
// 
//    Author: Sam Gerené, Alex Vorobiev, Alexander van Delft, Nathanael Smiechowski.
// 
//    This file is part of DEHP Common Library
// 
//    The DEHPCommon is free software; you can redistribute it and/or
//    modify it under the terms of the GNU Lesser General Public
//    License as published by the Free Software Foundation; either
//    version 3 of the License, or (at your option) any later version.
// 
//    The DEHPCommon is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//    Lesser General Public License for more details.
// 
//    You should have received a copy of the GNU Lesser General Public License
//    along with this program; if not, write to the Free Software Foundation,
//    Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace DEHPCommon.Tests.UserInterfaces.ViewModels.Rows.ElementDefinitionTreeRows
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;

    using CDP4Common.CommonData;
    using CDP4Common.EngineeringModelData;
    using CDP4Common.SiteDirectoryData;
    using CDP4Common.Types;

    using CDP4Dal;
    using CDP4Dal.Permission;

    using DEHPCommon.UserInterfaces.ViewModels.Rows.ElementDefinitionTreeRows;

    using Moq;

    using NUnit.Framework;

    /// <summary>
    /// Suite of tests for the <see cref="ParameterStateRowViewModelTestFixture"/>
    /// </summary>
    [TestFixture]
    public class ParameterStateRowViewModelTestFixture
    {
        private readonly Uri uri = new Uri("http://www.rheagroup.com");
        private ConcurrentDictionary<CacheKey, Lazy<Thing>> cache;
        private Mock<IPermissionService> permissionService;
        private Mock<ISession> session;

        private Participant participant;
        private Person person;
        private DomainOfExpertise activeDomain;
        private DomainOfExpertise otherDomain;

        private SiteDirectory siteDirectory;
        private EngineeringModelSetup engineeringModelSetup;
        private EngineeringModel engineeringModel;
        private Iteration iteration;
        private Option option;
        private ElementDefinition elementDefinition;
        private ElementDefinition otherElementDefinition;
        private ElementUsage elementUsage;
        private ActualFiniteStateList actualStates;
        private ActualFiniteState actualFiniteState0;
        private ActualFiniteState actualFiniteState1;

        [SetUp]
        public void SetUp()
        {
            this.cache = new ConcurrentDictionary<CacheKey, Lazy<Thing>>();
            this.permissionService = new Mock<IPermissionService>();
            this.permissionService.Setup(x => x.CanRead(It.IsAny<Thing>())).Returns(true);
            this.permissionService.Setup(x => x.CanWrite(It.IsAny<Thing>())).Returns(true);

            this.session = new Mock<ISession>();
            this.session.Setup(x => x.PermissionService).Returns(this.permissionService.Object);

            this.activeDomain = new DomainOfExpertise(Guid.NewGuid(), null, this.uri) { Name = "active", ShortName = "active" };
            this.otherDomain = new DomainOfExpertise(Guid.NewGuid(), null, this.uri) { Name = "other", ShortName = "other" };

            this.person = new Person(Guid.NewGuid(), null, this.uri) { GivenName = "test", Surname = "test" };
            this.participant = new Participant(Guid.NewGuid(), null, this.uri) { Person = this.person, SelectedDomain = this.activeDomain };
            this.session.Setup(x => x.ActivePerson).Returns(this.person);
            this.session.Setup(x => x.OpenIterations).Returns(new Dictionary<Iteration, Tuple<DomainOfExpertise, Participant>>());

            this.engineeringModelSetup = new EngineeringModelSetup(Guid.NewGuid(), null, this.uri);
            this.engineeringModelSetup.Participant.Add(this.participant);
            
            this.siteDirectory = new SiteDirectory(Guid.NewGuid(), this.cache, this.uri);
            this.engineeringModel = new EngineeringModel(Guid.NewGuid(), this.cache, this.uri);
            this.engineeringModel.EngineeringModelSetup = this.engineeringModelSetup;
            this.iteration = new Iteration(Guid.NewGuid(), this.cache, this.uri);
            this.option = new Option(Guid.NewGuid(), this.cache, this.uri) { Name = "TestName" };
            this.elementDefinition = new ElementDefinition(Guid.NewGuid(), this.cache, this.uri);
            this.otherElementDefinition = new ElementDefinition(Guid.NewGuid(), this.cache, this.uri);
            this.elementUsage = new ElementUsage(Guid.NewGuid(), this.cache, this.uri);
            this.elementUsage.ElementDefinition = this.otherElementDefinition;
            this.elementDefinition.ContainedElement.Add(this.elementUsage);

            var possibleFiniteStateList = new PossibleFiniteStateList(Guid.NewGuid(), this.cache, this.uri)
            {
                Name = "State"
            };

            var possibleFiniteState0 = new PossibleFiniteState(Guid.NewGuid(), this.cache, this.uri) { Name = "state0" };
            possibleFiniteStateList.PossibleState.Add(possibleFiniteState0);

            var possibleFiniteState1 = new PossibleFiniteState(Guid.NewGuid(), this.cache, this.uri) { Name = "state1" };
            possibleFiniteStateList.PossibleState.Add(possibleFiniteState1);

            this.actualFiniteState0 = new ActualFiniteState(Guid.NewGuid(), this.cache, this.uri)
            {
                PossibleState = 
                {
                    possibleFiniteState0
                }
            };

            this.actualFiniteState1 = new ActualFiniteState(Guid.NewGuid(), this.cache, this.uri)
            {
                PossibleState = 
                {
                    possibleFiniteState1
                }
            };

            this.actualStates = new ActualFiniteStateList(Guid.NewGuid(), this.cache, this.uri)
            {
                ActualState = { this.actualFiniteState0, this.actualFiniteState1 },
                PossibleFiniteStateList = { possibleFiniteStateList }
            };
            
            this.elementDefinition.Parameter.Add(new Parameter(Guid.NewGuid(), this.cache, this.uri)
                {
                    StateDependence = this.actualStates,
                    ParameterType = new SampledFunctionParameterType(Guid.NewGuid(), this.cache, this.uri)
                    {
                        Name = "DassParameter",
                        DependentParameterType =
                        {
                            new DependentParameterTypeAssignment(Guid.NewGuid(), this.cache, this.uri)
                            {
                                ParameterType = new BooleanParameterType(Guid.NewGuid(), this.cache, this.uri)
                                {
                                    Name = "ptb"
                                }
                            }
                        },
                        IndependentParameterType =
                        {
                            new IndependentParameterTypeAssignment(Guid.NewGuid(), this.cache, this.uri)
                            {
                                ParameterType = new BooleanParameterType(Guid.NewGuid(), this.cache, this.uri)
                                {
                                    Name = "ptb"
                                }
                            }
                        }
                    },
                    ValueSet = 
                    {
                        new ParameterValueSet()
                        {
                        Computed = new ValueArray<string>(new [] {"True","False"}),
                        ActualState = this.actualStates.ActualState.First()
                        },
                        new ParameterValueSet()
                        {
                            Computed = new ValueArray<string>(new [] {"True","False"}),
                            ActualState = this.actualStates.ActualState.Last()
                        }
                    }
                });


            this.engineeringModel.Iteration.Add(this.iteration);
            this.iteration.Option.Add(this.option);
            this.iteration.Element.Add(this.elementDefinition);
            this.iteration.Element.Add(this.otherElementDefinition);
        }

        [TearDown]
        public void TearDown()
        {
            CDPMessageBus.Current.ClearSubscriptions();
        }

        [Test]
        public void VerifyThatSampledFunctionParameteTypeThatHaveStateDependencyAreBuilt()
        {
            var row = new ElementDefinitionRowViewModel(this.elementDefinition, this.activeDomain, this.session.Object, null);

            Assert.IsNotNull(row.ThingStatus);
            Assert.IsNotEmpty(row.ContainedRows);
            var firstStateRow = row.ContainedRows.FirstOrDefault()?.ContainedRows[0];
            var secondStateRow = row.ContainedRows.FirstOrDefault()?.ContainedRows[1];
            Assert.IsNotNull(firstStateRow);
            Assert.IsNotNull(secondStateRow);
            Assert.IsTrue(firstStateRow is ParameterStateRowViewModel && secondStateRow is ParameterStateRowViewModel);
            
            Assert.AreEqual(this.actualFiniteState0.Name, 
                ((ParameterStateRowViewModel)firstStateRow).State);
            
            Assert.AreEqual(this.actualFiniteState1.Name, 
                ((ParameterStateRowViewModel)secondStateRow).State);
        }
    }
}