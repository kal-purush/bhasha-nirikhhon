from django import forms

class GetInitialInterest(forms.Form):
    interests = forms.MultipleChoiceField(
        choices=(("Happening", "Happening"),
                ("Member Led", "Member Led"),
                ("Caring", "Caring"),
                ("Sharing", "Sharing"),
                ("Learning", "Learning"),
                ("Working", "Working"),
                ("Democracy", "Democracy")), #first value is the actual value that gets submitted while the second one is visible in UI.
        widget=forms.CheckboxSelectMultiple,
    )
