using System.ComponentModel.DataAnnotations;
namespace DDVTracker.Models
{
    public class Character
    {
        /// <summary>
        /// The Character's Id number
        /// </summary>
        [Key]
        public int CharacterId { get; set; }

        /// <summary>
        /// The version of the game the character is included in
        /// </summary>
        [Required]
        public int GameVersionId { get; set; }

        /// <summary>
        /// The name of the character
        /// </summary>
        [Required]
        public string CharacterName { get; set; }

        /* WILL BE IMPLEMENTED LATER
        /// <summary>
        /// to save the character's image
        /// </summary>
        public byte[] CharacterImage { get; set; }
        */

        /// <summary>
        /// For user to note whether they have unlocked the character
        /// </summary>
        public bool isUnlocked { get; set; }

        /// <summary>
        /// For user to track Character's level
        /// </summary>
        public int CharacterLevel { get; set; }

        /// <summary>
        /// For user to note the skill they've assigned to the character
        /// </summary>
        public string AssignedSkill { get; set; }

        /// <summary>
        /// For user to note the character's favorite
        /// gifts for the day. 3 items reset daily
        /// </summary>
        public string FavoriteThing1 { get; set; }
        public string FavoriteThing2 { get; set; }
        public string FavoriteThing3 { get; set; }

    }
}