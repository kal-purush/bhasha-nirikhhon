namespace AES.Entities.Shared.Net45.Models
{
    using System;
    using System.Collections.Generic;
    using System.Data.Entity.Spatial;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
	using TrackableEntities.Client;

    public partial class AspNetRole : EntityBase
    {
        public AspNetRole()
        {
            AspNetUsers = new ChangeTrackingCollection<AspNetUser>();
        }

		public string Id
		{ 
			get { return _Id; }
			set
			{
				if (Equals(value, _Id)) return;
				_Id = value;
				NotifyPropertyChanged();
			}
		}
		private string _Id;

        [Required]
        [StringLength(256)]
		public string Name
		{ 
			get { return _Name; }
			set
			{
				if (Equals(value, _Name)) return;
				_Name = value;
				NotifyPropertyChanged();
			}
		}
		private string _Name;

		public ChangeTrackingCollection<AspNetUser> AspNetUsers
		{
			get { return _AspNetUsers; }
			set
			{
				if (Equals(value, _AspNetUsers)) return;
				_AspNetUsers = value;
				NotifyPropertyChanged();
			}
		}
		private ChangeTrackingCollection<AspNetUser> _AspNetUsers;
    }
}