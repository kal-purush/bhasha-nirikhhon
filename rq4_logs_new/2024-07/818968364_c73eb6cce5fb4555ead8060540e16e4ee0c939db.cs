using Repositories;
using Repositories.Entities;
using Repositories.Interfaces;
using System.Collections.Generic;

namespace Services
{
    public class ProfileService
    {
        private readonly IProfileRepository _profileRepo;
        private readonly INoteRepository _noteRepo;
        public ProfileService(IProfileRepository profileRepo, INoteRepository noteRepo)
        {
            _profileRepo = profileRepo ?? throw new ArgumentNullException(nameof(profileRepo));
            _noteRepo = noteRepo;
        }

        public bool AddProfile(Profile profile)
        {
            if (profile == null) throw new ArgumentNullException(nameof(profile));
            if (_profileRepo.GetProfilesByName(profile.ProfileName).ToArray().Length > 0) return false;
            return _profileRepo.AddProfile(profile);
        }

        public bool DeleteProfile(Profile profile)
        {
            if (profile == null) throw new ArgumentNullException(nameof(profile));
            List<Note> listNote = _noteRepo.GetNotesByProfileId(profile.ProfileId).ToList();
            foreach (Note note in listNote)
            {
                _noteRepo.DeleteNote(note);
            }
            return _profileRepo.DeleteProfile(profile);
        }

        public IEnumerable<Profile> GetAllProfiles()
        {

            return _profileRepo.GetAllProfiles();
        }

        public IEnumerable<Profile> GetProfilesByName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Invalid profile name", nameof(name));
            return _profileRepo.GetProfilesByName(name);
        }

        public bool UpdateProfile(Profile profile)
        {
            if (profile == null) return false;
            var oldProfile = _profileRepo.GetProfileById(profile.ProfileId);
            if (oldProfile == null) return false;
            return _profileRepo.UpdateProfile(profile);
        }
    }
}