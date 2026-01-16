import requests

# Target URL
url = "http://challenges.0x0539.net:3002/"

# Mersenne Twister state
MT_STATE = [0] * 624  # Internal state array for mt_rand
index = 0

# Initialize session to manage cookies
session = requests.Session()

def php_mt_rand(seed, rounds):
    """
    Predict mt_rand value using the Mersenne Twister algorithm.

    Args:
        seed (int): Seed value for mt_rand.
        rounds (int): Upper limit for mt_rand range.

    Returns:
        int: Predicted random number.
    """
    global MT_STATE, index

    # Initialize the MT array
    MT_STATE[0] = seed & 0xFFFFFFFF
    for i in range(1, 624):
        MT_STATE[i] = (1812433253 * (MT_STATE[i - 1] ^ (MT_STATE[i - 1] >> 30)) + i) & 0xFFFFFFFF

    def extract_number():
        """Extract the next number from the MT array."""
        global MT_STATE, index

        if index == 0:
            twist()

        y = MT_STATE[index]
        y ^= (y >> 11)
        y ^= (y << 7) & 0x9D2C5680
        y ^= (y << 15) & 0xEFC60000
        y ^= (y >> 18)

        index = (index + 1) % 624
        return y & 0x7FFFFFFF

    def twist():
        """Twist the MT array."""
        global MT_STATE, index

        for i in range(624):
            y = (MT_STATE[i] & 0x80000000) + (MT_STATE[(i + 1) % 624] & 0x7FFFFFFF)
            MT_STATE[i] = MT_STATE[(i + 397) % 624] ^ (y >> 1)
            if y % 2 != 0:
                MT_STATE[i] ^= 0x9908B0DF

    return (extract_number() % rounds) + 1


def automate_luck_tester(seed):
    """
    Automate the Luck Tester game.

    Args:
        seed (int): Initial seed for mt_rand.
    """
    streak = 0

    while streak < 100:
        # Get the current round and streak from the server
        response = session.get(url)
        cookies = session.cookies.get_dict()
        rounds = int(cookies.get("rounds", 1))

        # Predict the random number
        rnd = php_mt_rand(seed, rounds)

        # Submit the guess
        guess_payload = {"guess": rnd}
        response = session.post(url, data=guess_payload)
        response_text = response.text

        # Check response
        if "Congratulations" in response_text:
            streak += 1
            print(f"[+] Streak: {streak}, Guessed Correctly: {rnd}")
        else:
            print("[-] Guess failed. Resetting...")
            session.post(url, data={"reset": "1"})
            streak = 0

    print("[!] FLAG obtained!")


# Replace with the correct seed if known
seed = 123456  # This needs to be the seed used by the server
automate_luck_tester(seed)
