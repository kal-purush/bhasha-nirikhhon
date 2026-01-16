using Microsoft.EntityFrameworkCore;
using QuiryForum.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QuiryForum.Data
{
    /// <summary>
    /// DB helper for Answers
    /// </summary>
    public class AnswerDB
    {
        /// <summary>
        /// Adds an answer to the database. Automatically sets
        /// the ID.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static async Task<Answer> AddAsync(Answer a, QuiryContext context)
        {
            await context.AddAsync(a);
            await context.SaveChangesAsync();
            return a;
        }

        /// <summary>
        /// Updates the specified answer in the database.
        /// </summary>
        /// <param name="answer"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static async Task<Answer> EditAnswer(Answer answer, QuiryContext context)
        {
            context.Update(answer);
            await context.SaveChangesAsync();
            return answer;
        }

        /// <summary>
        /// Deletes the answer with the specified ID from the database.
        /// </summary>
        /// <param name="ID"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static async Task DeleteByID(int ID, QuiryContext context)
        {
            Answer answer = new Answer()
            {
                PostID = ID
            };
            context.Entry(answer).State = EntityState.Deleted;
            await context.SaveChangesAsync();
        }

        /// <summary>
        /// Gets a list of answers to the question with the given ID.
        /// </summary>
        /// <param name="questionID"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static async Task<List<Answer>> GetAnswersTo(int questionID, QuiryContext context)
        {
            List<Answer> answers = await context.Answers
                                                 .OrderBy(a => a.PostingDate)
                                                 .ToListAsync();
            return answers;
        }
    }
}