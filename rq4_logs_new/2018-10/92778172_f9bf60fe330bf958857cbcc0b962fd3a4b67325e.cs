using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using LifeLike.Data.Models;
using LifeLike.Repositories;
using Microsoft.AspNetCore.Http;

namespace LifeLike.Repositories.ViewModel
{
    public class Gallery
    {
        public long Id { get; set; }
        public string Title { get; set; }
        public DateTime Created { get; set; }
        public string Description { get; set; }
        public string Place { get; set; }

        public IEnumerable<Photo> Photos { get; set; }
        public string ShortTitle { get; set; }

        public static UploadFileViewModel GetViewForUpload(Gallery model)
        {
            return new UploadFileViewModel
            {
                GalleryTitle=model.Title,
                GalleryId=model.Id             
            };
        }

       
        // public static Gallery Get(GalleryEntity model)
        // {
        //     if (model == null) return null;
        //     return new Gallery
        //     {
        //         Id=model.Id,
        //         ShortTitle = model.ShortTitle,
        //         Title       = model.Title,
        //         Created = model.Created,
        //         Description =  model.Description,
        //         Place = model.Place,
        //         Photos = GetPhotoViewModels(model),
        //         SelectedPhoto=GetSelectedPhoto(model)

        //     };
        // }

        private static IEnumerable<Photo> GetPhotoViewModels(Gallery model)
        {
            return model.Photos ?? new List<Photo>();
        }

        private static string GetSelectedPhoto(Gallery model)
        {
            var selectedPhoto = model?.Photos?.FirstOrDefault() != null
                ?model.Photos.FirstOrDefault().Url
                : Path.Combine(PhotoRepository.PhotoPath, "logo.png");
 
            return selectedPhoto;
        }

        public string SelectedPhoto { get; set; }
    }

    public class UploadFileViewModel
    {
        public IFormFile Photo { get; set; }

        public long GalleryId { get; set; }
        public string GalleryTitle { get; set; }
        public string Title { get; set; }
    }
}