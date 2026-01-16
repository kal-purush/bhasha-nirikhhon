import spacy
from spacy.tokens import Span

# Load the pre-trained model
nlp = spacy.load("en_core_web_sm")

# Process the text
text = "Microsoft was founded by Bill Gates and Paul Allen. They are also known for their significant contributions to technology. Recently, Microsoft has acquired LinkedIn and is working on new AI technologies."
doc = nlp(text)

# Initialize a dictionary to store unique entities and their labels
entities_dict = {}

# Iterate through entities to add them to the dictionary
for ent in doc.ents:
    # Check if the entity is already in the dictionary
    if ent.text not in entities_dict:
        entities_dict[ent.text] = ent.label_
    else:
        # If it's already there, you might want to handle label conflicts
        # For simplicity, we'll keep the first encountered label
        pass

# Manually adjust labels if necessary  OPTIONAL
entities_dict['LinkedIn'] = 'ORG'  # Adjust LinkedIn to ORG
entities_dict['AI'] = 'TECHNOLOGY'  # Adjust AI to TECHNOLOGY or any other label you prefer

# Print the unique entities and their labels
for text, label in entities_dict.items():
    print(text, label)
#Note that model will recognize the entities based on the entity list in the pre-trained model used.
# If you want specific entity recongnition you can manually adjust labels.