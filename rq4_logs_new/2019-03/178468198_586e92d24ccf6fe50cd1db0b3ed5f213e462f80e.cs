using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DevExpress.Xpo;
using DevExpress.Xpo.DB;
using OSModel.OSDB;

namespace OSConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            XpoDefault.Session = null;
            ConnectionHelper.Connect(AutoCreateOption.DatabaseAndSchema, false);



            using (UnitOfWork uow = new UnitOfWork())
            {


                XPQuery<XPOColConfGroup> colconfgroup = uow.Query<XPOColConfGroup>();
                XPQuery<XPOColConference> team = uow.Query<XPOColConference>();
                XPQuery<XPOColConfDiv> tags = uow.Query<XPOColConfDiv>();
                XPQuery<XPOFacility> facil = uow.Query<XPOFacility>();
                XPQuery<XPOUniversity> univ = uow.Query<XPOUniversity>();
                XPQuery<XPOSportType> st = uow.Query<XPOSportType>();
                colconfgroup.Where(n => n.Oid > -1).Count();
                st.Where(n => n.Oid > -1).Count();
                univ.Where(n => n.Oid > -1).Count();


                //var xxx = from x in facil
                //          where x.YearBuilt == 1940
                //          select new
                //          {
                //              Univesity = x.XPOCollegeTeams.
                //          };

                //var facilityTag = (from x in tags
                //                   where x.Tag == "FB"
                //                   select x).SingleOrDefault(); ;


                //var grassSurface =  from myTags in tagMap
                //                    where myTags.TagID == surfaceTag
                //                    select myTags;

                //var fbTeams = from myTags in collegeTeam
                //              where myTags.Tag == facilityTag
                //              select myTags;

                //              //join teams in collegeTeam on myTags.EntityID equals teams.University
                //              //where myTags.TagID == surfaceTag && teams.Tag == facilityTag
                //              //select new
                //              //{
                //              //    University = teams.University.University,

                //              //};

                //XPQuery<OSModel.OSDB.XPOTags> tags1 = uow.Query<OSModel.OSDB.XPOTags>();
                //int x1 = tags1.Count();

                //var x = from myX in results
                //        select myX.

                // ----------- Tables ----------- //
                //XPOUniversityFacility myFac = new XPOUniversityFacility(uow);
                //myFac.Name = "Bryand Denny";
                //myFac.Capacity = 97456;
                //myFac.Surface = "Grass";



                //myTables.TableID = 1;

                //// ----------- Tables ----------- //
                //XPOSportType myTables = new XPOSportType(uow);
                //myTables.Type = "PRO";
                //myTables.Description = "Pro Sport";
                //myTables.Save();

                //myTables = new XPOSportType(uow);
                //myTables.Type = "COL";
                //myTables.Description = "College Sport";
                //myTables.Save();

                //myTables = new XPOSportType(uow);
                //myTables.Type = "OLY";
                //myTables.Description = "Olympic Sport";
                //myTables.Save();


                //myTables = new XPOTables(uow);
                //myTables.Table = "XPOProFranchise";
                //myTables.TableID = 2;

                //myTables = new XPOTables(uow);
                //myTables.Table = "XPOUniversityFacility";
                //myTables.TableID = 3;



                // -----------University---------- - //
                //XPOUniversity myCTeam = new XPOUniversity(uow);
                //myCTeam.University = "University of Florida";

                //XPOTeam myTeam = new XPOTeam(uow);
                //myTeam.TeamName = "Florida Gators";


                //XPOFacility facil = new XPOFacility(uow);
                //facil.Capacity = 88980;
                //facil.FacilityName = "Ben Hill Griffen Stadium";
                //facil.YearBuilt = 1940;

                //XPOCollegeTeam myCoTeam1 = new XPOCollegeTeam(uow);
                //myCoTeam1.Facility = facil;
                //myCoTeam1.TeamName = myTeam;
                //myCoTeam1.University = myCTeam;








                // ----------- Tags ----------- //
                //XPOTags myTags1 = new XPOTags(uow);
                //myTags1.Tag = "FB";
                //myTags1.Parent = null;
                //myTags1.Level = 0;
                //myTags1.Description = "Football";

                //XPOTags myTags2 = new XPOTags(uow);
                //myTags2.Tag = "CFB";
                //myTags2.Parent = 1;
                //myTags2.Level = 1;
                //myTags2.Description = "College Football";











                #region OldChit
                //XPOUniversity myUniv = new XPOUniversity(uow);
                //myUniv.University = "University of Florida";
                //myUniv.AthleticDirector = "Jermey Foley";

                //XPOAddress myAddress = new XPOAddress(uow);
                //myAddress.Address1 = "234 Archer Rd";
                //myUniv.Address = myAddress;

                //XPOState myState = new XPOState(uow);
                //myState.State = "Florida";

                //myUniv.Address.State = myState;
                #endregion


                uow.CommitChanges();

            }
        }
    }
}