import pyttsx3
import speech_recognition as sr
import random
import tkinter as tk
from tkinter import Toplevel, messagebox
import datetime
import webbrowser
import nltk
from nltk.stem import WordNetLemmatizer
import json
import pickle
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from PIL import Image, ImageTk
from tkinter import scrolledtext
from web import get_ai_response

# Download NLTK resources
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')  # For wordnet with multilingual support

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

# Load intents file
data_file = open("intents.json").read()
intents = json.loads(data_file)

# Prepare data
words = []
classes = []
documents = []
ignore_words = ['?', '!']

# Tokenize and process the data
for intent in intents['intents']:
    for pattern in intent['patterns']:
        # Tokenize each word in the pattern
        w = nltk.word_tokenize(pattern)
        words.extend(w)
        documents.append((w, intent['tag']))
        if intent['tag'] not in classes:
            classes.append(intent['tag'])

# Lemmatize and lower each word and remove duplicates
words = [lemmatizer.lemmatize(w.lower()) for w in words if w not in ignore_words]
words = sorted(list(set(words)))

# Sort classes
classes = sorted(list(set(classes)))

# Save words and classes for later use
pickle.dump(words, open('texts.pkl', 'wb'))
pickle.dump(classes, open('labels.pkl', 'wb'))

# Prepare training data (bag of words and output tags)
training = []
output_empty = [0] * len(classes)

# Prepare the training set
for doc in documents:
    # Initialize our bag of words
    bag = []
    pattern_words = doc[0]  # tokenized pattern words
    pattern_words = [lemmatizer.lemmatize(word.lower()) for word in pattern_words]
    
    # Create a bag of words array with 1 if word matches, else 0
    for w in words:
        bag.append(1) if w in pattern_words else bag.append(0)
    
    # Output row for the tag
    output_row = list(output_empty)
    output_row[classes.index(doc[1])] = 1
    
    # Append the bag of words and output to training data
    training.append([bag, output_row])

# Shuffle and convert into numpy arrays (make sure all inner lists have the same length)
random.shuffle(training)

# Separate the training data and labels
train_x = [item[0] for item in training]
train_y = [item[1] for item in training]

# Convert to numpy arrays
train_x = np.array(train_x)
train_y = np.array(train_y)

# Define PyTorch Dataset
class ChatDataset(torch.utils.data.Dataset):
    def __init__(self, X, y):
        self.X = X
        self.y = y

    def __len__(self):
        return len(self.X)

    def __getitem__(self, index):
        return torch.tensor(self.X[index], dtype=torch.float32), torch.tensor(self.y[index], dtype=torch.float32)  # Fix here

# Prepare training data and labels
train_data = ChatDataset(train_x, train_y)
train_loader = DataLoader(train_data, batch_size=5, shuffle=True)

# Define the model using PyTorch
class ChatBotModel(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(ChatBotModel, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x

# Initialize the model, loss function, and optimizer
model = ChatBotModel(len(train_x[0]), 64, len(classes))
criterion = nn.CrossEntropyLoss()
optimizer = optim.SGD(model.parameters(), lr=0.01)

# Train the model
epochs = 200
for epoch in range(epochs):
    for data in train_loader:
        inputs, labels = data
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, torch.max(labels, 1)[1])
        loss.backward()
        optimizer.step()

    if epoch % 10 == 0:
        print(f'Epoch {epoch}/{epochs}, Loss: {loss.item()}')

# Save the model after training
torch.save(model.state_dict(), 'model.pth')
print("Model trained and saved!")

# Chat function for Tkinter integration
def chat_response(user_input):
    # Tokenize the input
    message_words = nltk.word_tokenize(user_input)
    message_words = [lemmatizer.lemmatize(word.lower()) for word in message_words]
    
    # Create a bag of words
    bag = [0] * len(words)
    for w in message_words:
        if w in words:
            bag[words.index(w)] = 1
    
    # Convert to tensor
    input_tensor = torch.tensor(bag, dtype=torch.float32).unsqueeze(0)  # Add batch dimension
    
    # Predict
    output = model(input_tensor)
    
    # Ensure the output is of shape (batch_size, num_classes)
    if len(output.shape) == 1:
        output = output.unsqueeze(0)  # Add batch dimension if it's missing
    
    # Get the predicted class (tag)
    _, predicted = torch.max(output, dim=1)
    
    # Get the tag
    tag = classes[predicted.item()]
    
    # Find the response for the tag
    for intent in intents['intents']:
        if intent['tag'] == tag:
            response = random.choice(intent['responses'])
            break
    return response

# Tkinter GUI setup
def send_message():
    user_input = user_entry.get()
    if user_input.lower() == 'quit':
        root.quit()
    else:
        chat_window.config(state=tk.NORMAL)
        chat_window.insert(tk.END, "You: " + user_input + '\n')
        response = chat_response(user_input)
        chat_window.insert(tk.END, "Bot: " + response + '\n\n')
        chat_window.config(state=tk.DISABLED)
        user_entry.delete(0, tk.END)
        chat_window.yview(tk.END)
        update_history(user_input, response)
        speak(response)  # Speak the response

# User Profile to track user information
user_profile = {
    "username": "",
    "age": 0,
    "disability": None,
    "emotional_state": "",
    "therapy_ready": False,
    "location": "",
    "emotional_journey": [],
}

# Function to speak text
def speak(text):
    engine.say(text)  # Speak the text
    engine.runAndWait()

# Function to setup the text-to-speech engine
def setup_comforting_voice():
    global engine
    engine = pyttsx3.init()
    engine.setProperty('rate', 150)  # Speed of speech
    engine.setProperty('volume', 1)  # Volume level (0.0 to 1.0)
    voices = engine.getProperty('voices')
    engine.setProperty('voice', voices[1].id)  # Choose a voice (0 for male, 1 for female)

setup_comforting_voice()

recognizer = sr.Recognizer()

# Function to get user input via speech recognition
def get_user_input():
    with sr.Microphone() as source:
        print("Listening...")
        recognizer.adjust_for_ambient_noise(source)
        audio = recognizer.listen(source)
        try:
            user_input = recognizer.recognize_google(audio)
            print(f"You said: {user_input}")
            return user_input.lower()
        except sr.UnknownValueError:
            return "Sorry, I didn't understand. Please try again."
        except sr.RequestError:
            return "Could not process your request. Check your internet connection."

# Function to fetch AI-generated response
def get_bot_response(user_input):
    # Call the function from the Confluence\web.py to get a response from the trained AI model
    return get_ai_response(user_input)

# Function to fetch doctors based on location
def find_doctors():
    if user_profile["location"]:
        query = f"Mental health doctors in {user_profile['location']}"  # Search for doctors based on the city
        webbrowser.open(f"https://www.google.com/search?q={query}")  # Open the search result in browser
        return f"Here are some mental health doctors in {user_profile['location']}. Check your browser."
    else:
        return "Please enter your location first."

# Function to show doctors
def show_doctor_options():
    speak("Searching for doctors in your area...")
    response = find_doctors()
    chat_window.config(state=tk.NORMAL)
    chat_window.insert(tk.END, f"Bot: {response}\n\n")
    chat_window.config(state=tk.DISABLED)
    speak(response)

def show_helplines():
    helplines = """
    Mental Health Helplines in India:

    1. Kiran Helpline: 1800-599-0019 (Available 9:00 AM - 9:00 PM)
    2. Vandrevala Foundation: 91-9820466726 (Available 24/7)
    3. AASRA Foundation: 91-9820466726 (Available 24/7)

    Helpline Websites:
    1. [Kiran Helpline](https://www.kiranspecialist.org/)
    2. [Vandrevala Foundation](https://vandrevalafoundation.org/)
    3. [AASRA Foundation](https://www.aasra.org/)
    
    For more, please check online mental health support numbers in your region.
    """
    chat_window.config(state=tk.NORMAL)
    chat_window.insert(tk.END, f"Bot: {helplines}\n\n")
    chat_window.config(state=tk.DISABLED)
    speak(helplines)  # Speak the helpline information

def open_support_group_website():
    webbrowser.open("https://www.thelivelovelaughfoundation.org/")

def show_progress_report():
    progress_report = """
    How to Track Your Mental Health Progress:

    1. Track your emotions and moods daily using a journal or a mood tracking app.
    2. Reflect on your experiences to identify patterns and triggers.
    3. Set small goals to improve your mental health and track your progress.
    4. Share your feelings with a therapist or counselor during your sessions.
    5. Regularly review how you are doing, and don't be afraid to reach out for help.
    """
    chat_window.config(state=tk.NORMAL)
    chat_window.insert(tk.END, f"Bot: {progress_report}\n\n")
    chat_window.config(state=tk.DISABLED)
    speak(progress_report)

def show_self_care_tips():
    self_care_tips = """
    Self-care Tips for Mental Well-being:

    1. Practice mindfulness and deep breathing exercises.
    2. Stay connected with loved ones and share your feelings.
    3. Engage in physical activities like walking, yoga, or any hobbies that relax you.
    4. Maintain a balanced diet and ensure you get enough sleep.
    5. Limit your time on social media and take breaks from screens.

    Take time for yourself and focus on your mental health daily.
    """
    chat_window.config(state=tk.NORMAL)
    chat_window.insert(tk.END, f"Bot: {self_care_tips}\n\n")
    chat_window.config(state=tk.DISABLED)
    speak(self_care_tips)



def update_history(user_input, bot_response):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_profile["emotional_journey"].append({
        "timestamp": timestamp,
        "User": user_input,
        "Bot": bot_response
    })

def show_history():
    # Create a pop-out window to display history
    history_window = Toplevel(root)
    history_window.title("Conversation History")
    history_window.geometry("500x400")

    # Create a scrolled text box to display the history
    history_box = scrolledtext.ScrolledText(history_window, width=60, height=20)
    history_box.pack(padx=10, pady=10)

    # Display the timestamped history
    for entry in user_profile["emotional_journey"]:
        history_box.insert(tk.END, f"{entry['timestamp']}\nUser: {entry['User']}\nBot: {entry['Bot']}\n\n")

    history_box.config(state=tk.DISABLED)
    speak("Here is your conversation history.")  # Speak that the history is being shown

# Tkinter setup
root = tk.Tk()
root.title("Mental Health ChatBot")

try:
    bg_image = Image.open("background.jpg")  # Add a calming background
    bg_image = bg_image.resize((1500, 900), Image.Resampling.LANCZOS)
    bg_photo = ImageTk.PhotoImage(bg_image)

    bg_label = tk.Label(root, image=bg_photo)
    bg_label.place(relwidth=1, relheight=1)
except FileNotFoundError:
    print("Background image not found. Using default background.")
    root.config(bg="orange") 

# Create chat window (text area)
chat_window = scrolledtext.ScrolledText(root, wrap=tk.WORD, state=tk.DISABLED, width=50, height=20)
chat_window.grid(row=0, column=0, columnspan=2, padx=10, pady=10)

# Create a frame for buttons and other widgets
button_frame = tk.Frame(root)
button_frame.grid(row=1, column=0, columnspan=2, padx=10, pady=10)

# Entry box for user to type messages
user_entry = tk.Entry(root, width=45)
user_entry.grid(row=2, column=0, padx=10, pady=10)

# Send button
send_button = tk.Button(button_frame, text="Send", width=20, command=send_message)
send_button.grid(row=0, column=0, padx=10)

# Button to show helplines
helpline_button = tk.Button(button_frame, text="Helplines", width=20, command=show_helplines)
helpline_button.grid(row=0, column=1, padx=10)

# Show doctor options button
doctor_button = tk.Button(button_frame, text="Find Doctors", width=20, command=show_doctor_options)
doctor_button.grid(row=0, column=2, padx=10)

# Show history button
history_button = tk.Button(button_frame, text="Show History", width=20, command=show_history)
history_button.grid(row=1, column=0)

# User Profile Details - Adding the name, age, emotional state, and location input fields

# Entry for Name
name_label = tk.Label(root, text="Enter your Name:")
name_label.grid(row=3, column=0, padx=10, pady=5)
name_entry = tk.Entry(root, width=30)
name_entry.grid(row=3, column=1, padx=10, pady=5)

# Entry for Age
age_label = tk.Label(root, text="Enter your Age:")
age_label.grid(row=4, column=0, padx=10, pady=5)
age_entry = tk.Entry(root, width=30)
age_entry.grid(row=4, column=1, padx=10, pady=5)

# Entry for Emotional State
emotional_state_label = tk.Label(root, text="Enter your Emotional State:")
emotional_state_label.grid(row=5, column=0, padx=10, pady=5)
emotional_state_entry = tk.Entry(root, width=30)
emotional_state_entry.grid(row=5, column=1, padx=10, pady=5)

# Entry for Location
location_label = tk.Label(root, text="Enter your Location:")
location_label.grid(row=6, column=0, padx=10, pady=5)
location_entry = tk.Entry(root, width=30)
location_entry.grid(row=6, column=1, padx=10, pady=5)

def enable_voice_input():
    if visually_impaired_var.get():
        speak("Voice input has been enabled. Please speak your query.")
        user_input = get_user_input()
        send_message(user_input)

visually_impaired_var = tk.BooleanVar()
visually_impaired_checkbox = tk.Checkbutton(root, text="I am visually impaired", variable=visually_impaired_var, command= enable_voice_input)
visually_impaired_checkbox.grid(row=7, column=0, columnspan=2, pady=10)

# Function to enable voice input if visually impaired


# Submit button to save the user profile information
def save_user_profile():
    user_profile["username"] = name_entry.get()
    user_profile["age"] = age_entry.get()
    user_profile["emotional_state"] = emotional_state_entry.get()
    user_profile["location"] = location_entry.get()
    chat_window.config(state=tk.NORMAL)
    chat_window.insert(tk.END, f"Bot: Profile updated: {user_profile}\n\n")
    chat_window.config(state=tk.DISABLED)
    speak("Profile updated successfully.")

profile_button = tk.Button(root, text="Save Profile", command=save_user_profile)
profile_button.grid(row=8, column=0, columnspan=2, pady=10)

def send_history():
    # Show the "History sent" pop-up message
    messagebox.showinfo("History Sent", "Your conversation history has been sent.")

send_history_button = tk.Button(button_frame, text="Send History", command=send_history)
send_history_button.grid(row=1, column=1)

support_groups_button = tk.Button(root, text="Mental Health Support Groups", command=open_support_group_website)
support_groups_button.grid(row=1, column=2)

progress_report_button = tk.Button(root, text="Track Progress", command=show_progress_report)
progress_report_button.grid(row=2, column=1)


# Start the Tkinter GUI loop
root.mainloop()