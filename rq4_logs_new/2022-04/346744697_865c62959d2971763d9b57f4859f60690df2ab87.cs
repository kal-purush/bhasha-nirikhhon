namespace Personal
{
    public static class BlogHelper
    {
        private static BlogContext blogCtx;
        public static void Configure(BlogContext _blogCtx)
        {
            blogCtx = _blogCtx;
        }
        /// <summary>
        /// Converts string to tag registries and map to the post entries
        /// </summary>
        /// <param name="etiquetas">Input string to convert</param>
        /// <param name="idPost">Id of post entry</param>
        public static void ProcessTags(string etiquetas, string idPost)
        {
            List<string> usedTags = new();
            foreach (var tags in etiquetas.Split(';'))
            {
                var trimmedTag = tags.Trim();
                if (trimmedTag != String.Empty) //No empty tags
                {
                    usedTags.Add(trimmedTag);
                    if (!blogCtx.Etiquetas.Any(t => t.Name == trimmedTag)) // create tag if not exist
                    {
                        blogCtx.Etiquetas.Add(new Tag() { Name = trimmedTag });
                        blogCtx.SaveChanges();
                    }
                    // now, check if has in the map.
                    if (!blogCtx.EtiquetasEntradas.Any(et => et.Tag == trimmedTag && et.IdPost == idPost))
                    {
                        blogCtx.EtiquetasEntradas.Add(new PostTags() { IdPost = idPost, Tag = trimmedTag });
                        blogCtx.SaveChanges();
                    }
                }
            }
            //then remove unused tags.
            var unused = blogCtx.EtiquetasEntradas.Where(et => et.IdPost == idPost && !usedTags.Contains(et.Tag));
            blogCtx.EtiquetasEntradas.RemoveRange(unused);
            //save
            blogCtx.SaveChanges();
        }
        /// <summary>
        /// Creates (if doesn't exist) a date entry on DB and maps to Post
        /// </summary>
        /// <param name="Date">Date of post</param>
        /// <param name="idPost">Id of post</param>
        public static void ProcessDates(DateTime Date, string idPost)
        {
            var datename = Date.ToString("yy-M");
            if (!blogCtx.Fechas.Any(d => d.Name == datename))
            {
                blogCtx.Fechas.Add(new MonthYear() { Name = datename });
                blogCtx.SaveChanges();
            }

            var post = blogCtx.FechasEntradas.Find(idPost);
            if (post != null)
            {
                post.IdMonthYear = datename;
            }
            else
            {
                blogCtx.FechasEntradas.Add(new PostMonthYear
                {
                    IdPost = idPost,
                    IdMonthYear = datename
                });
            }
            blogCtx.SaveChanges();
        }
        /// <summary>
        /// this method is to read from XML blogspot history and creates local registry.
        /// </summary>
        public static void Importar()
        {
            //Background:
            //Para importar el archivo xml-json- a este blog, debo guardarlos en la base de datos.

            //var json = System.IO.File.ReadAllText(Environment.WebRootPath + "/blogpost/xmltojson.json");
            //var data = System.Text.Json.JsonSerializer.Deserialize<Root>(json);
            //foreach (var ent in data.feed.entry)
            //{
            //    if (ent.author.name == "Shinji")
            //    {
            //        var cont = ent.content.ToString();

            //        if (cont.Contains("jarm.yo@gmail.com"))
            //        {
            //            _DbContext.Entradas.Add(new Post()
            //            {
            //                Id = Guid.NewGuid().ToString(),
            //                Fecha = ent.published,
            //                Titulo = ent.title,
            //                Contenido = ent.content.ToString()
            //            });
            //        }
            //    }
            //}

            // _DbContext.SaveChanges();
        }
    }
}