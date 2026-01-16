from algorithms.one_time_pad import OneTime


def test_encrypt():
    onetime = OneTime()
    cipher, key = onetime.encrypt("Hello, World!")

    assert onetime.decrypt(cipher, key) == "Hello, World!"
    assert onetime.decrypt_2(cipher, key) == "Hello, World!"

test_encrypt()