using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Maybe.Tests.FakeStuff.Entities;

namespace Maybe.Tests.FakeStuff.Repositories;

public class PersonRepository
{
    private readonly List<PersonEntity> _people = new();

    public PersonEntity Create(PersonEntity person)
    {
        _people.Add(person);

        return person;
    }

    public IEnumerable<PersonEntity> CreateMany(IEnumerable<PersonEntity> people)
    {
        var peopleList = people.ToList();

        peopleList.ForEach(person =>
        {
            person.Guid = Guid.NewGuid();

            _people.Add(person);
        });

        return peopleList;
    }

    public PersonEntity? Get(Guid guid)
    {
        return _people.FirstOrDefault(person => person.Guid == guid);
    }

    public List<PersonEntity> GetAll()
    {
        return _people;
    }
}