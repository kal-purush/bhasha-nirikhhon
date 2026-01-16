from django.db import models

class Person(models.Model):
    full_name = models.CharField(max_length=150)
    phone_number = models.CharField(max_length=25)
    email = models.EmailField()
    
    def __unicode__(self):
        return self.full_name

class Address(models.Model):
    street = models.CharField(max_length=150)
    city = models.CharField(max_length=100)
	flat_number = models.CharField(max_length=25)

    person = models.OneToOneField(Person, null=True)

    def __unicode__(self):
        return self.street
class TechnicalSkills(models.Model):
    technology = models.CharField(max_length=200)
	

class Education(models.Model):
    School = models.CharField(max_length=100)
	Intermediate=models.CharField(max_length=100)
	Degree=models.CharField(max_length=100)
	
	
	
	
		
		
		
		
		
		