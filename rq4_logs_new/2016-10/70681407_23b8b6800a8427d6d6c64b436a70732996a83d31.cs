using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SharePoint.Client;

namespace RatingManipulationC
{
	class ChangeRating
	{
		bool hasUserRated = false;
		Random random = new Random();
		int sumOfRatings = 0;
		int userRatingIndex = 0;
		int numberOfTries = 5;
		float newAverageRating = 0;
		int numberOfRatings = 0;
		string newRatingsFromUser = string.Empty;
		string newRatings = string.Empty;
		List<FieldUserValue> newUsersRated = new List<FieldUserValue>();
		public void NewRatings(ClientContext clientContext, List list, ListItem item, User user)
		{
			Console.Clear();
			Console.WriteLine("Site: " + clientContext.Web.Title);
			Console.WriteLine("List: " + list.Title);
			Console.WriteLine("Item: " + item.DisplayName);
			Console.WriteLine("User: " + user.Title);
			Console.WriteLine();
			FieldUserValue[] ratedUsers = item["RatedBy"] as FieldUserValue[];
			if (ratedUsers != null)
				for (int i = 0; i < ratedUsers.Length; i++)
					if (ratedUsers[i].LookupValue == user.Title)
					{
						userRatingIndex = i;
						hasUserRated = true;
						break;
					}
			if (hasUserRated)
				Rerate(item, user);
			else
				Rate(item, user, ratedUsers);
			Console.WriteLine("Users Rating " + newRatingsFromUser + ",\nRatings= " + newRatings + "\nRatings Count= " + numberOfRatings + ",\nAverage Ratings= " + newAverageRating);
			item.Update();
		}

		//function to re-rate or skip rating the item
		private void Rerate(ListItem item, User user)
		{
			int randomStarRating = random.Next(1, 5);
			string[] ratings = item["Ratings"].ToString().Split(',');
			numberOfRatings = int.Parse(item["RatingCount"].ToString());
			string ratedValue = ratings[userRatingIndex];
			Console.WriteLine("The User " + user.Title + " has already Rated the item with " + ratedValue + " star Rating\nDo you want to enter new Rating Y/N");
			string reRate = Console.ReadLine();
			if (reRate == "y" || reRate == "Y" || reRate == "yes" || reRate == "Yes")
			{
				for (int i = 0; i < numberOfRatings; i++)
					if (i == userRatingIndex)
					{
						Console.WriteLine("Enter the new Star Rating or skip(EnterKey) to generate Random Star Rating");
						string skipReadingRatings = Console.ReadLine();
						do
						{
							if (skipReadingRatings == "" || numberOfTries == 1)
							{
								newRatingsFromUser = randomStarRating.ToString();
								newRatings += newRatingsFromUser + ",";
								sumOfRatings += randomStarRating;
							}
							else
							{
								newRatingsFromUser = skipReadingRatings;
								if (newRatingsFromUser == "1" || newRatingsFromUser == "2" || newRatingsFromUser == "3" || newRatingsFromUser == "4" || newRatingsFromUser == "5")
								{
									newRatings += newRatingsFromUser + ",";
									sumOfRatings += int.Parse(newRatingsFromUser);
								}
								else
								{
									Console.WriteLine("The ratings entered is incorrect please enter numbers from 1 to 5 only\nOr\nskip(EnterKey) to generate Random Star Rating");
									skipReadingRatings = Console.ReadLine();
								}
							}
							numberOfTries--;
						} while (newRatingsFromUser != "1" && newRatingsFromUser != "2" && newRatingsFromUser != "3" && newRatingsFromUser != "4" && newRatingsFromUser != "5" && skipReadingRatings != "" && numberOfTries != 0);
					}
					else
					{
						newRatings += ratings[i] + ",";
						sumOfRatings += int.Parse(ratings[i].ToString());
					}
				item["Ratings"] = newRatings;
				item["AverageRating"] = (float)(sumOfRatings / numberOfRatings);
			}
			else
			{
				newRatingsFromUser = ratedValue;
				newRatings = item["Ratings"].ToString();
				newAverageRating = float.Parse(item["AverageRating"].ToString());
			}
		}

		//Function To rate the item
		private void Rate(ListItem item, User user, FieldUserValue[] ratedUsers)
		{
			int randomStarRating = random.Next(1, 5);
			string[] ratings = item["Ratings"] != null ? item["Ratings"].ToString().Split(',') : null;
			newRatings = item["Ratings"] != null ? item["Ratings"].ToString() : string.Empty;
			newAverageRating = item["AverageRating"] == null ? 0 : float.Parse(item["AverageRating"].ToString());
			numberOfRatings = item["RatingCount"] == null ? 0 : int.Parse(item["RatingCount"].ToString());
			Console.WriteLine("The User " + user.Title + " has not Rated the item.\nEnter the new Star Rating or skip(EnterKey) to generate Random Star Rating");
			string skip = Console.ReadLine();
			do
			{
				if (skip == "" || numberOfTries == 1)
				{
					newRatingsFromUser = randomStarRating.ToString();
					newRatings += newRatingsFromUser + ",";
					newAverageRating = ((newAverageRating * numberOfRatings) + randomStarRating) / (numberOfRatings + 1);
					//or
					//newAverageRating = newAverageRating + ((randomStarRating - newAverageRating) / (numberOfRatings + 1));
				}
				else
				{
					newRatingsFromUser = skip;
					if (newRatingsFromUser == "1" || newRatingsFromUser == "2" || newRatingsFromUser == "3" || newRatingsFromUser == "4" || newRatingsFromUser == "5")
					{
						newRatings += newRatingsFromUser + ",";
						sumOfRatings += int.Parse(newRatingsFromUser);
						newAverageRating = ((newAverageRating * numberOfRatings) + int.Parse(newRatingsFromUser)) / (numberOfRatings + 1);
					}
					else
					{
						Console.WriteLine("The ratings entered is incorrect please enter numbers from 1 to 5 only or skip(EnterKey) to generate Random Star Rating");
						skip = Console.ReadLine();
					}
				}
				numberOfTries--;
			} while (newRatingsFromUser != "1" && newRatingsFromUser != "2" && newRatingsFromUser != "3" && newRatingsFromUser != "4" && newRatingsFromUser != "5" && skip != ""&& numberOfTries != 0);
			numberOfRatings += 1;
			item["RatingCount"] = numberOfRatings.ToString();
			item["Ratings"] = newRatings;
			item["AverageRating"] = newAverageRating.ToString();
			if (ratedUsers != null)
				foreach (FieldUserValue ratedUser in ratedUsers)
					newUsersRated.Add(ratedUser);
			newUsersRated.Add(FieldUserValue.FromUser(user.LoginName));
			item["RatedBy"] = newUsersRated;
		}
	}
}