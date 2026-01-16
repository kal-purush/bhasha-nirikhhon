from pylatex import Section, Command, NoEscape

def add_acknowledgements(doc):
        # Start ignoring content for word count
    doc.append(NoEscape(r'%TC:ignore'))
    
    # Force a new page before acknowledgements
    doc.append(NoEscape(r'\newpage'))
    
    # Center the section title
    doc.append(NoEscape(r'\begin{center}'))
    
    # Begin the acknowledgements with an unnumbered section and no ToC entry
    doc.append(NoEscape(r'\section*{Acknowledgements}'))
    doc.append(r'This work is dedicated to Lupe, Armando, Lupita and Pepe. Thank you for everything.')
    
    # End the centering
    doc.append(NoEscape(r'\end{center}'))
    
    # Add the acknowledgements text
    doc.append(r'The Author is grateful for the financial support of the British Foreign, Commonwealth and Development Office Chevening Scholarship (2023/2024), and for the encouragement and patience of the Academic Staff at LSE Department of International Development.')

    # End ignoring content for word count
    doc.append(NoEscape(r'%TC:endignore'))