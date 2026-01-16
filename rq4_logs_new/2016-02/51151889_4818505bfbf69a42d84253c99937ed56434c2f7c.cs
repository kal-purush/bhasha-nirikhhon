namespace BoardGames.Services.Data
{
    using BoardGames.Services.Data.Contracts;
    using System;
    using System.Linq;
    using BoardGames.Data.Models;
    using BoardGames.Data.Common;

    public class TutorialsService : ITutorialsService
    {
        private readonly IDbRepository<Tutorial> tutorials;

        public TutorialsService(IDbRepository<Tutorial> tutorials)
        {
            this.tutorials = tutorials;
        }

        public void Add(string title, string gameTitle, string content, string creatorId, int? imageId = null)
        {
            var newTutorial = new Tutorial
            {
                Title = title,
                Game = gameTitle,
                Content = content,
                AuthorId = creatorId
            };

            if (imageId != null)
            {
                newTutorial.ImageId = (int)imageId;
            }
            else
            {
                newTutorial.ImageId = 1;
            }

            this.tutorials.Add(newTutorial);
            this.tutorials.Save();
        }

        public void Delete(int id)
        {
            var tutorialToBeDeleted = this.tutorials.GetById(id);
            this.tutorials.Delete(tutorialToBeDeleted);
            this.tutorials.Save();
        }

        public void Edit(int id, string title, string gameTitle, string content, int? imageId = null)
        {
            var tutorialToBeEdited = this.tutorials.GetById(id);
            tutorialToBeEdited.Title = title;
            tutorialToBeEdited.Game = gameTitle;
            tutorialToBeEdited.Content = content;

            if (imageId != null)
            {
                tutorialToBeEdited.ImageId = (int)imageId;
            }

            this.tutorials.Save();
        }

        public IQueryable<Tutorial> GetAll()
        {
            return this.tutorials.All();
        }

        public Tutorial GetById(int id)
        {
            return this.tutorials.GetById(id);
        }
    }
}