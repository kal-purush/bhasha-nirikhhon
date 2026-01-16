using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Media;
using Microsoft.Xna.Framework.Content;

namespace SkoleProjekt_GodtNavn
{
    public class SpriteContainer
    {
        public Dictionary<string, Texture2D> soleSprite = new Dictionary<string, Texture2D>();
        public SpriteFont spriteFont;

        public void LoadContent(ContentManager content)
        {

            // Font
            spriteFont = content.Load<SpriteFont>("Font/NormalFont");

            // Load in Image
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/helmet_slot"), "helmet_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/chest_slot"), "chest_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/pants_slot"), "pants_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/gloves_slot"), "gloves_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/boots_slot"), "boots_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/sword_slot"), "sword_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/staff_slot"), "staff_slot");

            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/helmet"), "helmet");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/chest"), "chest");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/pants"), "pants");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/gloves"), "gloves");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/boots"), "boots");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/sword"), "sword");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Character panel/staff"), "staff");

            // Inventory
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/apple"), "apple");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/background_bag"), "background_bag");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/bag"), "bag");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/empty_slot"), "empty_slot");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/empty_slot_bagbar"), "empty_slot_bagbar");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/epmtyFrame"), "epmtyFrame");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/full_slot_bag"), "full_slot_bag");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/healthPotion"), "healthPotion");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Inventory/key"), "key");


            // Quest Log
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/arrow_down"), "arrow_down");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/arrow_up"), "arrow_up");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/button_1"), "button_1");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/button_2"), "button_2");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/close_button_"), "close_button_");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/frame_1"), "frame_1");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/frame_2"), "frame_2");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/frame_3"), "frame_3");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/quest_log"), "quest_log");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/slider"), "slider");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/slider_bar"), "slider_bar");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/text_frame_1"), "text_frame_1");
            AddSoleSprite(content.Load<Texture2D>("Texture/UI/Quest log/text_frame_2"), "text_frame_2");
        }


        public void AddSoleSprite(Texture2D texture2D, string name)
        {
            soleSprite.Add(name, texture2D);
        }
    }
}