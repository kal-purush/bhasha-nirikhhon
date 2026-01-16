using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sharpAdvising
{
   public class CourseGraph
    {
      public CourseGraph()
       {
           adj = new GraphNode[MATRIXSIZE, MATRIXSIZE];
       }

       public void addNode(string department, string numberID)
       {

           bool alreadyHaveIt = false;
           foreach (GraphNode element in courses)
           {
               if (element.department == department && element.number == numberID) {
                   alreadyHaveIt = true;
               }
           }

           ///TODO: if this class doesnt have any pre-reqs or are the pre-reqs are already completed we need to end this recursion

           if (!(alreadyHaveIt))
           {
               GraphNode nodeToAdd = new GraphNode(department, numberID);
               int i = 0;
               for (; i < MATRIXSIZE; i++)
               {
                   for (int j = 0; j < MATRIXSIZE; j++)
                   {
                       if (adj[i, j] != null)
                       {
                           break;
                       }
                   }
               }

               nodeToAdd.row = i;
               courses.Add(nodeToAdd);

               //Read the pre-reqs here then call the function on itself 
           }

                      
       }

       public GraphNode [,] adj;
       const int MATRIXSIZE = 100;
       public List<GraphNode> courses;
    }
}





