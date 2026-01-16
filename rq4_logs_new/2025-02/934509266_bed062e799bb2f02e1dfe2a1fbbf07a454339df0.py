import os
import sqlite3
import streamlit as st
from datetime import datetime
from dotenv import load_dotenv
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from PIL import Image
import io

# Load environment variables
load_dotenv()

# Configure API keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not GEMINI_API_KEY:
    st.error("Gemini API key not found. Please add it to your .env file.")
    st.stop()

if not YOUTUBE_API_KEY:
    st.error("YouTube API key not found. Please add it to your .env file.")
    st.stop()

# Configure Gemini AI
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")
vision_model = genai.GenerativeModel("gemini-1.5-pro")

class IndianRecipeSystem:
    def __init__(self):
        """Initialize the Indian recipe system with enhanced categorization."""
        self.conn = sqlite3.connect("indian_recipes.db", check_same_thread=False)
        self.create_recipes_table()
        
        self.youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        
        # Enhanced categorization
        self.cuisine_regions = [
            "Any",
            "Andhra",
            "Telangana",
            "South Indian",
            "North Indian",
            "Bengali",
            "Gujarati",
            "Maharashtrian",
            "Rajasthani",
            "Punjab"
        ]
        
        self.meal_categories = {
            "Breakfast": ["Dosa", "Idli", "Upma", "Poha", "Paratha"],
            "Lunch": ["Rice Based", "Roti Based", "Thali"],
            "Dinner": ["Light Meals", "Full Course", "One Pot Meals"],
            "Snacks": ["Tea Time", "Evening Snacks", "Quick Bites"]
        }
        
        self.cooking_styles = [
            "Traditional",
            "Modern Fusion",
            "Quick & Easy",
            "Healthy",
            "Low Oil",
            "One Pot"
        ]

    def create_recipes_table(self):
        """Create enhanced database table for storing Indian recipes."""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS saved_recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_name TEXT NOT NULL,
                recipe_name_telugu TEXT,
                region TEXT NOT NULL,
                meal_category TEXT NOT NULL,
                cooking_style TEXT NOT NULL,
                ingredients TEXT NOT NULL,
                instructions TEXT NOT NULL,
                video_link TEXT,
                cooking_time INTEGER,
                created_date TEXT NOT NULL
            )
        ''')
        self.conn.commit()

    def identify_ingredients_from_image(self, uploaded_file):
        """Enhanced ingredient identification with detailed categorization."""
        try:
            image = Image.open(uploaded_file)
            
            prompt = """
            Analyze this image and provide a detailed breakdown of Indian ingredients in the following format:

            1. Main Ingredients:
               - List each visible main ingredient with quantity
               - Specify condition (fresh, dried, processed)
            
            2. Spices and Seasonings:
               - List visible spices with approximate quantities
               - Note whole vs ground form
            
            3. Aromatics and Herbs:
               - Identify fresh herbs, onions, garlic, ginger etc.
               - Specify quantity and condition
            
            4. Additional Components:
               - Any visible oils, ghee, or cooking mediums
               - Special ingredients or regional specifics
            
            For each ingredient, provide:
            - Exact or estimated quantity
            - Condition/form
            - Any visible quality indicators
            - Common Indian name if applicable
            
            Format each category separately and be very precise with measurements.
            """
            
            response = vision_model.generate_content([
                prompt,
                {"mime_type": "image/jpeg", "data": uploaded_file.getvalue()}
            ])
            
            return response.text.strip()
        except Exception as e:
            st.error(f"Error identifying ingredients: {str(e)}")
            return None

    def search_telugu_recipe_video(self, recipe_name, region, style):
        """Enhanced YouTube search for more relevant Telugu recipe videos."""
        try:
            style_term = "traditional" if style == "Traditional" else style.lower()
            search_query = f"{recipe_name} {region} {style_term} recipe telugu vantalu తెలుగు వంటలు"
            
            request = self.youtube.search().list(
                part="snippet",
                q=search_query,
                type="video",
                maxResults=3,
                relevanceLanguage="te",
                regionCode="IN",
                videoDefinition="high"
            )
            response = request.execute()
            
            videos = []
            if response['items']:
                for item in response['items']:
                    video_id = item['id']['videoId']
                    title = item['snippet']['title']
                    videos.append({
                        'id': video_id,
                        'title': title,
                        'url': f"https://www.youtube.com/watch?v={video_id}"
                    })
            return videos
        except HttpError as e:
            st.error(f"Error searching YouTube: {str(e)}")
            return None

    def generate_recipe(self):
        """Streamlined recipe generation with all options visible at once."""
        st.title("🍲 Indian Recipe Generator")
        
        # Initialize session state for ingredients
        if 'identified_ingredients' not in st.session_state:
            st.session_state.identified_ingredients = None
        if 'analyzing_ingredients' not in st.session_state:
            st.session_state.analyzing_ingredients = False
        if 'preferences_set' not in st.session_state:
            st.session_state.preferences_set = False

        # Image Upload Section
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("1. Upload Ingredients Photo")
            uploaded_file = st.file_uploader("Choose an image...", type=["jpg", "jpeg", "png"])
            
            if uploaded_file:
                st.image(uploaded_file, caption="Uploaded Ingredients", use_column_width=True)
                
                # Analyze ingredients button
                if st.button("Analyze Ingredients") and not st.session_state.analyzing_ingredients:
                    st.session_state.analyzing_ingredients = True
                    with st.spinner("Analyzing ingredients... Please wait..."):
                        identified_ingredients = self.identify_ingredients_from_image(uploaded_file)
                        if identified_ingredients:
                            st.session_state.identified_ingredients = identified_ingredients
                    st.session_state.analyzing_ingredients = False

        # Recipe Preferences Section (always visible)
        with col2:
            st.subheader("2. Recipe Preferences")
            
            # Create form for all preferences
            with st.form(key='recipe_preferences'):
                region = st.selectbox("Select Region", self.cuisine_regions)
                meal_category = st.selectbox("Meal Category", list(self.meal_categories.keys()))
                sub_category = st.selectbox(
                    "Sub Category",
                    self.meal_categories[meal_category]
                )
                cooking_style = st.selectbox("Cooking Style", self.cooking_styles)
                cooking_time = st.slider("Cooking Time (minutes)", 15, 120, 30, step=5)
                spice_level = st.slider("Spice Level", 1, 5, 3)
                
                # Submit button for preferences
                preferences_submitted = st.form_submit_button("Set Preferences")
                if preferences_submitted:
                    st.session_state.preferences_set = True

        # Display identified ingredients if available
        if st.session_state.identified_ingredients:
            st.subheader("Identified Ingredients:")
            st.markdown(st.session_state.identified_ingredients)

        # Generate Recipe Button (enabled only when both ingredients and preferences are ready)
        if st.session_state.identified_ingredients and st.session_state.preferences_set:
            if st.button("Generate Recipe", type="primary"):
                prompt = f"""
                Create a detailed {region} Indian {meal_category} recipe ({sub_category}) 
                based on these identified ingredients:
                
                {st.session_state.identified_ingredients}
                
                Requirements:
                - Cooking Style: {cooking_style}
                - Maximum Time: {cooking_time} minutes
                - Spice Level: {spice_level}/5
                
                Format the response as:
                
                # [RECIPE NAME IN ENGLISH]
                # [RECIPE NAME IN TELUGU]
                
                ## Ingredients
                [List all ingredients with precise measurements]
                
                ## Preparation Steps (with time estimates)
                1. [Step-by-step instructions]
                
                ## Cooking Method
                1. [Detailed cooking steps]
                
                ## Tips & Variations
                - [Regional variations]
                - [Time-saving tips]
                - [Storage suggestions]
                
                ## Serving Suggestions
                - [Accompaniments]
                - [Plating suggestions]
                
                Note: Focus on {cooking_style} style and ensure total cooking time stays within {cooking_time} minutes.
                """
                
                with st.spinner("Creating your personalized recipe..."):
                    recipe_content = self.safe_generate_content(prompt)
                    if recipe_content:
                        st.markdown(recipe_content)
                        
                        # Extract recipe names
                        recipe_lines = recipe_content.split('\n')[:2]
                        recipe_name_en = recipe_lines[0].replace('#', '').strip()
                        recipe_name_te = recipe_lines[1].replace('#', '').strip()
                        
                        # Find matching videos
                        with st.spinner("Finding video tutorials..."):
                            videos = self.search_telugu_recipe_video(recipe_name_en, region, cooking_style)
                            
                        if videos:
                            st.subheader("వంటకం వీడియోలు / Recipe Videos")
                            for video in videos:
                                st.video(video['url'])
                                st.caption(video['title'])
                        
                        if st.button("Save Recipe"):
                            try:
                                cursor = self.conn.cursor()
                                cursor.execute('''
                                    INSERT INTO saved_recipes 
                                    (recipe_name, recipe_name_telugu, region, meal_category,
                                    cooking_style, ingredients, instructions, video_link,
                                    cooking_time, created_date)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    recipe_name_en, recipe_name_te, region, meal_category,
                                    cooking_style, st.session_state.identified_ingredients,
                                    recipe_content, videos[0]['url'] if videos else None,
                                    cooking_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                ))
                                self.conn.commit()
                                st.success("Recipe saved successfully!")
                                st.balloons()
                            except Exception as e:
                                st.error(f"Error saving recipe: {e}")
        elif not st.session_state.identified_ingredients:
            st.info("Please upload and analyze your ingredients first.")
        elif not st.session_state.preferences_set:
            st.info("Please set your recipe preferences.")

    def safe_generate_content(self, prompt):
        """Safely generate content using Gemini AI with error handling."""
        try:
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            st.error(f"Error generating content: {e}")
            return None

def main():
    """Enhanced main application with better UI organization."""
    st.set_page_config(
        page_title="Indian Recipe Generator",
        page_icon="🍲",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    system = IndianRecipeSystem()
    system.generate_recipe()

if __name__ == "__main__":
    main()