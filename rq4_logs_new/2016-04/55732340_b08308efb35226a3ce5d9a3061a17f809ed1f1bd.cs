using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRepository.Models
{


    public class RegattaDisplayManager
    {

        public RegattaDisplayManager()
        {
 
        }
  
 
        public Regatta Get(int RegattaID)
        {
            Regatta returnedRegatta = null;
            using (boxerdb db = new boxerdb())
            {
                returnedRegatta = db.Regattas.SingleOrDefault(x => x.Id == RegattaID);
            }
            return returnedRegatta;
        }

 

        internal List<Document> GetStories(int RegattaID)
        {
            List<Document> storyList = new List<Document>();
            using (boxerdb db = new boxerdb())
            {
                storyList = db.Document.Where(x => x.RegattaID == RegattaID).OrderBy(x=>x.DocumentDateTime).ToList();
            }
            return storyList;
        }

        internal List<Image> GetPhotos(int RegattaID)
        {
            List<Image> photoList = null;
            using (boxerdb db = new boxerdb())
            {
                photoList = db.Images.Where(x => x.RegattaID == RegattaID).OrderBy(x => x.Id).ToList();
            }
            return photoList;
        }

        internal List<CrewMember> GetCrew(int RegattaID)
        {
            List<CrewMember> crewList = null;

           // string SQL = string.Concat("SELECT CM.CrewName  FROM RegattaCrew RC Inner Join CrewMember CM ON CM.ID = RC.CrewMemberID where RC.RegattaID =", RegattaID)  ;


            using (boxerdb db = new boxerdb())
            {
               // crewList = db.CrewMembers.SqlQuery(SQL).ToList();
                crewList = (from c in db.CrewMembers
                            join rc in db.RegattaCrews on c.Id equals rc.CrewMemberId
                            where rc.RegattaId == RegattaID
                            select new CrewMember
                            {
                                CrewName = c.CrewName,
                                CrewEmail = c.CrewEmail
                            }).ToList();


               }
            return crewList;
        }


 



    }
}