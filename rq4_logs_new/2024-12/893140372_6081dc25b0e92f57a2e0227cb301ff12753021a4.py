import base64
import json
import numpy as np
from phe import paillier
from pathlib import Path


def generate_keys(public_path: Path, private_path: Path):
    public_path.mkdir(parents=True, exist_ok=True)
    private_path.mkdir(parents=True, exist_ok=True)
    
    public_key_file = public_path / "public_phe_key.json"
    private_key_file = private_path / "private_phe_key.json"
    
    if public_key_file.exists() and private_key_file.exists():
        print("PHE Keys already exist. Skipping key generation.")
        return
    
    public_key, private_key = paillier.generate_paillier_keypair()

    public_key_data = {"n": base64.b64encode(str(public_key.n).encode("utf-8")).decode("utf-8")}
    with open(public_key_file, "w") as pub_file:
        json.dump(public_key_data, pub_file)

    private_key_data = {
        "p": base64.b64encode(str(private_key.p).encode("utf-8")).decode("utf-8"),
        "q": base64.b64encode(str(private_key.q).encode("utf-8")).decode("utf-8"),
        "public_key_n": base64.b64encode(str(public_key.n).encode("utf-8")).decode("utf-8"),
    }
    with open(private_key_file, "w") as priv_file:
        json.dump(private_key_data, priv_file)



def load_public_key(public_path: Path):
    key_file = public_path / "public_phe_key.json"
    if not key_file.exists():
        raise FileNotFoundError(f"Public key file not found: {key_file}")
    
    # load public key
    with open(key_file, "r") as pub_file:
        public_key_data = json.load(pub_file)

    try:
        n = int(base64.b64decode(public_key_data["n"]).decode("utf-8"))
        return paillier.PaillierPublicKey(n=n)
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError("Invalid public key format.") from e


def decode_data(data, public_path: Path, private_path: Path):

    public_key = load_public_key(public_path)

    private_key_file = private_path / "private_phe_key.json"
    if not private_key_file.exists():
        raise FileNotFoundError(f"Private key file not found: {private_key_file}")

    # Load private key
    with open(private_key_file, "r") as priv_file:
        private_key_data = json.load(priv_file)

    try:
        p = int(base64.b64decode(private_key_data["p"]).decode("utf-8"))
        q = int(base64.b64decode(private_key_data["q"]).decode("utf-8"))
        private_key = paillier.PaillierPrivateKey(public_key=public_key, p=p, q=q)
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError("Invalid private key format.") from e

    # Decrypt the data
    decrypt_scalar = np.vectorize(private_key.decrypt)
    try:
        decrypted_data = decrypt_scalar(data)
    except Exception as e:
        raise ValueError("Error during decryption.") from e

    return decrypted_data