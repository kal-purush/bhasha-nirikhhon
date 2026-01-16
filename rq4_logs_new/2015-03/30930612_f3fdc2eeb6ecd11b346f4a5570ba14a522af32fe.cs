// source: http://paginatedcollection.codeplex.com/
// Paginatedcollection is licensed under the Microsoft Public License (MS-PL) which requires a full copy of the license when
// distributing source code.

// Microsoft Public License (MS-PL)

// This license governs use of the accompanying software. If you use the software, you
// accept this license. If you do not accept the license, do not use the software.

// 1. Definitions
// The terms "reproduce," "reproduction," "derivative works," and "distribution" have the
// same meaning here as under U.S. copyright law.
// A "contribution" is the original software, or any additions or changes to the software.
// A "contributor" is any person that distributes its contribution under this license.
// "Licensed patents" are a contributor's patent claims that read directly on its contribution.

// 2. Grant of Rights
// (A) Copyright Grant- Subject to the terms of this license, including the license conditions and limitations in section 3, each contributor grants you a non-exclusive, worldwide, royalty-free copyright license to reproduce its contribution, prepare derivative works of its contribution, and distribute its contribution or any derivative works that you create.
// (B) Patent Grant- Subject to the terms of this license, including the license conditions and limitations in section 3, each contributor grants you a non-exclusive, worldwide, royalty-free license under its licensed patents to make, have made, use, sell, offer for sale, import, and/or otherwise dispose of its contribution in the software or derivative works of the contribution in the software.

// 3. Conditions and Limitations
// (A) No Trademark License- This license does not grant you rights to use any contributors' name, logo, or trademarks.
// (B) If you bring a patent claim against any contributor over patents that you claim are infringed by the software, your patent license from such contributor to the software ends automatically.
// (C) If you distribute any portion of the software, you must retain all copyright, patent, trademark, and attribution notices that are present in the software.
// (D) If you distribute any portion of the software in source code form, you may do so only under this license by including a complete copy of this license with your distribution. If you distribute any portion of the software in compiled or object code form, you may only do so under a license that complies with this license.
// (E) The software is licensed "as-is." You bear the risk of using it. The contributors give no express warranties, guarantees or conditions. You may have additional consumer rights under your local laws which this license cannot change. To the extent permitted under your local laws, the contributors exclude the implied warranties of merchantability, fitness for a particular purpose and non-infringement.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GW2Trader.MVVM
{
    using System.Collections.ObjectModel;
    using System.ComponentModel;
    using System.Collections.Specialized;
    using System.Collections.Generic;

    /// <summary>
    /// This class represents a single Page collection, but have the entire items available inside
    /// </summary>

    public class PaginatedObservableCollection<T> : ObservableCollection<T>
    {
        #region Properties
        public int PageSize
        {
            get { return _itemCountPerPage; }
            set
            {
                if (value >= 0)
                {
                    _itemCountPerPage = value;
                    RecalculateThePageItems();
                    OnPropertyChanged(new PropertyChangedEventArgs("PageSize"));
                }
            }
        }

        public int CurrentPage
        {
            get { return _currentPageIndex; }
            set
            {
                if (value >= 0)
                {
                    _currentPageIndex = value;
                    RecalculateThePageItems();
                    OnPropertyChanged(new PropertyChangedEventArgs("CurrentPage"));
                }
            }
        }

        #endregion

        #region Constructor
        public PaginatedObservableCollection(IEnumerable<T> collection, int itemsPerPage = 10)
        {
            _currentPageIndex = 0;
            _itemCountPerPage = itemsPerPage;
            originalCollection = new List<T>(collection);
            RecalculateThePageItems();
        }

        public PaginatedObservableCollection(int itemsPerPage)
        {
            _currentPageIndex = 0;
            _itemCountPerPage = itemsPerPage;
            originalCollection = new List<T>();
        }
        public PaginatedObservableCollection()
        {
            _currentPageIndex = 0;
            _itemCountPerPage = 1;
            originalCollection = new List<T>();
        }
        #endregion

        #region private
        private void RecalculateThePageItems()
        {
            Clear();

            int startIndex = _currentPageIndex * _itemCountPerPage;

            for (int i = startIndex; i < startIndex + _itemCountPerPage; i++)
            {
                if (originalCollection.Count > i)
                    base.InsertItem(i - startIndex, originalCollection[i]);
            }
        }
        #endregion

        #region Overrides

        protected override void InsertItem(int index, T item)
        {
            int startIndex = _currentPageIndex * _itemCountPerPage;
            int endIndex = startIndex + _itemCountPerPage;

            //Check if the Index is with in the current Page then add to the collection as bellow. And add to the originalCollection also
            if ((index >= startIndex) && (index < endIndex))
            {
                base.InsertItem(index - startIndex, item);

                if (Count > _itemCountPerPage)
                    base.RemoveItem(endIndex);
            }

            if (index >= Count)
                originalCollection.Add(item);
            else
                originalCollection.Insert(index, item);
        }

        protected override void RemoveItem(int index)
        {
            int startIndex = _currentPageIndex * _itemCountPerPage;
            int endIndex = startIndex + _itemCountPerPage;
            //Check if the Index is with in the current Page range then remove from the collection as bellow. And remove from the originalCollection also
            if ((index >= startIndex) && (index < endIndex))
            {
                base.RemoveAt(index - startIndex);

                if (Count <= _itemCountPerPage)
                    base.InsertItem(endIndex - 1, originalCollection[index + 1]);
            }

            originalCollection.RemoveAt(index);
        }

        #endregion

        private List<T> originalCollection;
        private int _currentPageIndex;
        private int _itemCountPerPage;
    }
}