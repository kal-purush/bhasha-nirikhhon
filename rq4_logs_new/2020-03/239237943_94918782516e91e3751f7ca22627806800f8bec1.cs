using System;
using System.Collections.Generic;

namespace PatternDesignImageStorage
{
    public class ImageStorage
    {
        private readonly ICompressor _compressor;
        private readonly List<IFilter> _filters;
        
        public ImageStorage(ICompressor compressor )
        {
            if ( compressor == null )
                throw new NullReferenceException("Compressor cannot be null");
            
            _compressor = compressor;
            
            _filters = new List<IFilter>();
        }

        public void AddFilters(IFilter filter)
        {
            _filters.Add(filter);
        }
        public void Store(string fileName)
        {
            _compressor.Compress(fileName);

            if(_filters.Count <= 0)
                Console.WriteLine("No filters applied");
            
            foreach (var filter in _filters)
            {
                filter.Apply(fileName);
            }
            
        }
    }
}


// -	This ImageStorage class has _compressor, _filter as its properties. 
// This can be set through the constructor or the method that you need.
// -	ImageStorage class has a method called 'store'. 
// 'store' method gets the filename (do not worry about image type, just pass string as "fileName")
// -	When store method is executed, the logic under the method needs to have "DIFFERENT behavior" based on what compressor 
// is and what filter is. `store` logic is going to be like compress, and then apply filter
//
// Compressor example: jpg, png…
// Filter example: blackAndWhite, highContrast