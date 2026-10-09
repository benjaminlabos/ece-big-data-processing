"""Configuration commune au nouveau lab Kafka."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "gutenberg-book-lines"
BOOK_PATH = ROOT / "data" / "around_the_world_in_80_days.txt"
OUTPUT_PATH = ROOT / "output" / "livre_nettoye.txt"

