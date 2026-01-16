using Akavache;
using PDGT.Models;
using System;
using System.Reactive.Linq;
using System.Collections.Generic;
using System.Text;

namespace PDGT.Stores
{
    class QuestionnaireStore
    {
        public static async void SetPreQuestionnaireAsync(PreQuestionnaire p)
        {
            PreQuestionnaire CurrentPreQuestionnaire = p;
            await BlobCache.LocalMachine.InsertObject<PreQuestionnaire>("PreQuestionnaire", CurrentPreQuestionnaire);
        }

        public static PreQuestionnaire GetPreQuestionnaire()
        {
            PreQuestionnaire p = new PreQuestionnaire();

            try
            {
                p = BlobCache.LocalMachine.GetObject<PreQuestionnaire>("PreQuestionnaire").Wait();
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine("Nothing to get.");
            }
            return p;
        }
    }
}