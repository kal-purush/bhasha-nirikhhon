# tests/test_mailing.py
from toolbox_proj.mailing import verify_address

def test_verify_wrong_email():
    assert verify_address('sdrfrghedtr.com') == 'Bad Syntax'

def test_verify_correct_email():
    assert verify_address('sdrfrghedtr@gmail.com') == None