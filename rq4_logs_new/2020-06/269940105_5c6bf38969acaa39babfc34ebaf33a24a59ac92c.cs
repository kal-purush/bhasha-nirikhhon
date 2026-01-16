using Microsoft.EntityFrameworkCore;
using OrganisationArchive.DAL.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace OrganisationArchive.DAL
{
    public class DatabaseInitializer : IDatabaseInitializer
    {
        private OrganizationDbContext _context;
        public DatabaseInitializer(OrganizationDbContext context)
        {
            _context = context;
        }
        public async Task Seed()
        {
            _context.Organizations.Add(
                 new Organization
                 {
                     Name = "Apple Inc. (AAPL)",
                     Address = "Apple Park Way Cupertino, California, United States",
                     Work = "Apple Inc. is an American multinational technology company that designs, develops, and sells consumer electronics, computer software, and online services. It is considered one of the Big Tech technology companies, alongside Amazon, Google, Microsoft and Facebook."
                 });
            _context.Organizations.Add(
                new Organization
                {
                    Name = "World Bank Group",
                    Address = "Washington, District of Columbia (USA)",
                    Work = "Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text.",
                });
            _context.Organizations.Add(
                new Organization
                {
                    Name = "Microsoft Corp. (MSFT)",
                    Address = "Redmond, Washington, United States",
                    Work = "Microsoft Corporation is an American multinational technology company with headquarters in Redmond, Washington. It develops, manufactures, licenses, supports, and sells computer software, consumer electronics, personal computers, and related services."
                });
            _context.Organizations.Add(
                new Organization
                {
                    Name = "International Business Machines Corp. (IBM)",
                    Address = "New York, United States",
                    Work = "IBM is an integrated solutions and services company, also referred to as ''Big Blue.'' The company offers software and IT solutions for a broad range of uses, including healthcare, financial services, Internet of Things (IoT), weather, security, as well as cloud-computing services. The company is known for its powerful Watson computer, which offers a suite of enterprise-ready AI services, applications, and tools.",
                });
            _context.Organizations.Add(
                new Organization
                {
                    Name = "Intel Corp. (INTC)",
                    Address = "California, United States",
                    Work = "Intel is a premier global producer of computer chips and a provider of computing, networking, data storage, and communication solutions. The company offers platform products for the cloud, enterprise, and communication infrastructure markets. Intel provides flash memory, programmable semiconductors, and processors for notebooks, mobile devices, and desktop computers. The company is well known for its high-performance processors used in PCs worldwide by businesses and consumers."
                });
            _context.Organizations.Add(
                new Organization
                {
                    Name = "Uber Technologies Inc",
                    Address = "San Francisco, California, United States",
                    Work = "Uber Technologies, Inc., commonly known as Uber, is an American multinational ride-hailing company offering services that include peer-to-peer ridesharing, ride service hailing, food delivery, and a micromobility system with electric bikes and scooters"
                });
            _context.Organizations.Add(
                new Organization
                {
                    Name = "Oracle Corporation",
                    Address = "Redwood City, California, United States",
                    Work = "Oracle Corporation is an American multinational computer technology corporation headquartered in Redwood Shores, California. The company sells database software and technology, cloud engineered systems, and enterprise software products—particularly its own brands of database management systems."
                }
                );

            _context.People.Add(new Person()
            {
                Name = "Mariam",
                Lastname = "Zardiashvili",
                Gender = "Female",
                PersonalNumber = "12345678901",
                DateOfBirth = new DateTime(2000, 6, 17),
                City = "Tbilisi",
                PhoneNumber = "551204848"
            });
            _context.People.Add(
            new Person
            {
                Name = "Tamar",
                Lastname = "Nasrashvili",
                Gender = "Female",
                PersonalNumber = "09876543211",
                DateOfBirth = new DateTime(2000, 06, 11),
                City = "Tbilisi",
                PhoneNumber = "599878716"
            });
            _context.People.Add(
            new Person
            {
                Name = "Nata",
                Lastname = "Shengelia",
                Gender = "Female",
                PersonalNumber = "23415687990",
                DateOfBirth = new DateTime(1999, 07, 14),
                City = "Tbilisi",
                PhoneNumber = "567342456"
            });
           _context.People.Add(
           new Person
            {
                Name = "Oleg",
                Lastname = "Gugunava",
                Gender = "Male",
                PersonalNumber = "45632187922",
                DateOfBirth = new DateTime(1996, 09, 23),
                City = "Tbilisi",
                PhoneNumber = "598-70-57-93"
            }
            );
           _context.SaveChanges();
            _context.Database.EnsureCreated();
        }
    }
}