"""Lab, étape 3 : télécharger le livre en texte UTF-8 depuis Gutenberg."""
import hashlib
from urllib.request import urlopen

from config import BOOK_PATH

BOOK_URL = "https://www.gutenberg.org/ebooks/2154.txt.utf-8"


def main():
    with urlopen(BOOK_URL, timeout=30) as response:
        data = response.read()
    text = data.decode("utf-8-sig")
    if "*** START OF THE PROJECT GUTENBERG EBOOK" not in text:
        raise ValueError("La réponse ne contient pas le livre attendu.")
    BOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    BOOK_PATH.write_bytes(data)
    print(f"Livre téléchargé : {BOOK_PATH}")
    print(f"Taille : {len(data)} octets")
    print(f"SHA-256 : {hashlib.sha256(data).hexdigest()}")


if __name__ == "__main__":
    main()

