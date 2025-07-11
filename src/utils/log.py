from datetime import datetime
from print_color import print

def log(message: str, level: str = "info"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}",tag=level,tag_color={
        "info": "blue",
        "warn": "yellow",
        "error": "red",
        "success": "green"
    }.get(level, "white"),color="white")
