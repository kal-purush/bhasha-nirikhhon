from google.colab import auth
from googleapiclient.discovery import build
from ipywidgets import widgets, VBox
import time

# Authenticate and create the Google Docs service
auth.authenticate_user()
service = build('docs', 'v1')

def staged_upload(document_id, content_sections, interval):
    """Uploads content sections to a new Google Doc in intervals."""
    new_doc_title = 'Staged Document'

    # Create a new empty document
    new_doc = service.documents().create(body={'title': new_doc_title}).execute()
    new_doc_id = new_doc.get('documentId')

    # Copy sections with pauses
    for section in content_sections:
        requests = [
            {
                'insertText': {
                    'location': {'index': 1},  # Insert at the beginning 
                    'text': section + "\n\n"  # Add a newline for spacing
                }
            }
        ]
        service.documents().batchUpdate(documentId=new_doc_id, body={'requests': requests}).execute()
        time.sleep(interval)

# Input widgets
document_id_input = widgets.Text(
    value='',
    placeholder='Enter Google Document ID',
    description='Doc ID:',
    disabled=False
)

content_input = widgets.Textarea(
    value='',
    placeholder='Enter content separated by new lines for each section.',
    description='Content:',
    disabled=False
)

interval_input = widgets.IntSlider(
    value=60,
    min=5,
    max=300,
    step=5,
    description='Interval (s):'
)

button = widgets.Button(description="Upload Content")

# Display widgets
def on_button_clicked(b):
    document_id = document_id_input.value
    content_sections = content_input.value.split('\n')
    interval = interval_input.value
    staged_upload(document_id, content_sections, interval)
    print("Content uploaded successfully!")

button.on_click(on_button_clicked)

display(VBox([document_id_input, content_input, interval_input, button]))