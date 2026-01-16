package org.willowlms.actors;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.springframework.data.neo4j.annotation.*;
import org.willowlms.roles.Role;

@NodeEntity
public class Student implements User {
@GraphId Long id;
public String name;
public Role   role;

public Student()
{
}

public Student(String name, Role role)
{
	this.name = name;
	this.role = role;
}

@RelatedTo(type = "CLASSMATE", direction = Direction.BOTH)
public @Fetch Set <Student> classmates;

public void
hasClassWith(Student student)
{
	if (classmates == null) {
		classmates = new HashSet <Student>();
	}
	classmates.add(student);
}

public String
toString()
{
	String results = name + "'s teammates include\n";

	if (classmates != null) {
		for (Student student : classmates) {
			results += "\t- " + student.name + "\n";
		}
	}
	return results;
}

public long
getId()
{
	return id;
}
}