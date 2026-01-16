using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using PaymentContext.Domain.Entities;
using PaymentContext.Domain.Entities.Domain;
using PaymentContext.Domain.Enums;
using PaymentContext.Domain.ValueObjects;

namespace PaymentContext.Tests.Entites
{
    [TestClass]
    public class StudentTest
    {
        private Name name;
        private Document document;
        private Email email;
        private Address address;
        private Student student;
        private Subscription subscription;

        public StudentTest()
        {
            name = new Name("Bruce", "Wayne");
            document = new Document("123", EDocumentType.CPF);
            email = new Email("bat@mail.com");
            address = new Address("Rua", "1234", "Catolé", "João Pessoa", "PB", "Brasil", "58884000");
            student = new Student(name, document, email);
            subscription = new Subscription(null);
        }

        [TestMethod]
        public void ShouldReturnErrorWhenHadActiveSubscription() 
        {
            var payment = new PayPalPayment("123456", DateTime.Now, DateTime.Now.AddDays(5), 10, 10, "Wayne Corp", document, address, email);
            subscription.AddPayments(payment);
            student.AddSubscripton(subscription);
            student.AddSubscripton(subscription);
            
            Assert.IsTrue(student.Invalid);
        }
        public void ShouldReturnErrorWhenHadSubscriptionHasNoPayment() 
        {
            student.AddSubscripton(subscription);    
            Assert.IsTrue(student.Invalid);
        }

        public void SholdReturnSuccessWhenHadNoActiveSubscription() 
        {
            var payment = new PayPalPayment("123456", DateTime.Now, DateTime.Now.AddDays(5), 10, 10, "Wayne Corp", document, address, email);
            subscription.AddPayments(payment);
            student.AddSubscripton(subscription);
            
            Assert.IsTrue(student.Valid);
        }
    }
}