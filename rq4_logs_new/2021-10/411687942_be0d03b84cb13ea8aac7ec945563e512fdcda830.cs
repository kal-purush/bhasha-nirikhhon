using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Firestore;

namespace Domain.Setup
{
    public class Firebase : IFirebase
    {
        private static FirestoreDb _firestoreDatabase;

        public Firebase()
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + @"beefitmemberbookings-firebase.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", path);
            
            _firestoreDatabase = FirestoreDb.Create("beefitmemberbookings");
        }

        public async Task AddClass(Guid classId)
        {
            var collection = _firestoreDatabase.Collection("Classes").Document($"{classId}");
            Dictionary<string, List<string>> participants = new Dictionary<string, List<string>>
            {
                {"Participants", new List<string>()}
            };

            await collection.CreateAsync(participants);
        }

        public async Task DeleteBooking(string classId, string email)
        {
            var collection = _firestoreDatabase
                .Collection("Classes")
                .Document($"{classId}");

            var participants = await collection.GetSnapshotAsync();
            
            if (participants.Exists)
            {
                var participantsDictionary = participants.ToDictionary();
                var participantsList = participantsDictionary["Participants"] as List<object>;

                if (participantsList == null)
                    return;

                if (participantsList.Contains(email))
                {
                    participantsList.Remove(email);
                }
                else
                {
                    return;
                }
                
                participantsDictionary["Participants"] = participantsList;
                
                await collection.UpdateAsync(participantsDictionary);
            }
        }

        public async Task AddUserToClass(string classId, string userId)
        {
            var collection = _firestoreDatabase
                .Collection("Classes")
                .Document($"{classId}");

            var participants = await collection.GetSnapshotAsync();
            
            if (participants.Exists)
            {
                var participantsDictionary = participants.ToDictionary();
                var participantsList = participantsDictionary["Participants"] as List<object>;

                if (participantsList == null)
                    return;

                if (participantsList.Contains(userId))
                    return;
                
                participantsList.Add(userId);

                participantsDictionary["Participants"] = participantsList;
                
                await collection.UpdateAsync(participantsDictionary);
            }
        }
    }
}