"""Nettoyage simple inspiré des étapes du notebook word count du cours."""
import re

# Liste courte et explicite : pas de téléchargement NLTK supplémentaire.
STOP_WORDS = set("""
a an the and or but if then as at by for from in into of on to with
is are was were be been being it its this that these those
i me my we us our you your he him his she her they them their
s t d ll ve re m
""".split())


def clean_text(line):
    # Minuscules ; conserver seulement les mots (lettres Unicode), puis
    # retirer les mots courants et reconstruire une ligne aux espaces simples.
    words = re.findall(r"[^\W\d_]+", line.lower(), flags=re.UNICODE)
    return " ".join(word for word in words if word not in STOP_WORDS)


class GutenbergCleaner:
    """Ne conserver que le texte entre les deux balises Gutenberg."""

    def __init__(self):
        self.started = False
        self.finished = False

    def process(self, line):
        if line.startswith("*** START OF THE PROJECT GUTENBERG EBOOK"):
            self.started = True
            self.finished = False
            return ""
        if line.startswith("*** END OF THE PROJECT GUTENBERG EBOOK"):
            self.finished = True
            return ""
        if not self.started or self.finished:
            return ""
        return clean_text(line)

