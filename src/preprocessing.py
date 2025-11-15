import re
import sys
sys.path.append('scripts')
from config import config

EMOJI_PATTERN = re.compile(
    "(["
    "\U0001F1E0-\U0001F1FF"
    "\U0001F300-\U0001F5FF" 
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00002600-\U000027BF"
    "])", flags=re.UNICODE
)

URL_PATTERN = re.compile(r"((?:https?|ftp)://[^\s/$.?#].[^\s]*|www\.[^\s/$.?#].[^\s]*)", flags=re.IGNORECASE)
DOMAIN_PATTERN = re.compile(r"\b[\w-]+\.(?:ru|me|com|org|net|io)[^\s]*", flags=re.IGNORECASE)
TELEGRAM_PATTERN = re.compile(r"(?:t\.me/|telegram\.me/|tg://)[^\s]+", flags=re.IGNORECASE)
USERS_PATTERN = re.compile(r"@\w+")
HASHTAG_PATTERN = re.compile(r"#\w+")
MULTIPLE_DOTS_PATTERN = re.compile(r"\.{3,}")

def remove_emoji(text):
    return EMOJI_PATTERN.sub("", text)

def remove_urls(text):
    text = URL_PATTERN.sub("", text)
    text = DOMAIN_PATTERN.sub("", text)
    text = TELEGRAM_PATTERN.sub("", text)
    return text

def remove_hashtags(text):
    return HASHTAG_PATTERN.sub("", text)

def remove_users(text):
    return USERS_PATTERN.sub("", text)

def normalize_quotes(text):
    text = re.sub(r'[«»"''`´]', '"', text)
    if text.count('"') % 2 != 0:
        text = text.replace('"', ' ')
    return text

def fix_paragraphs(text):
    paragraphs = text.split("\n")
    cleaned = []
    for p in paragraphs:
        p = " ".join(p.split()).strip()
        if len(p) >= 5:
            cleaned.append(p)
    return "\n".join(cleaned)

def normalize_spaces(text):
    text = re.sub(r'\s+', ' ', text)
    text = MULTIPLE_DOTS_PATTERN.sub('...', text)
    return text.strip()

class TextProcessor:

    def __init__(self, processor_config=None):
        processor_config = config.get("preprocessing", {}).get("text_processor", {})
        
        self.pipeline = (
            remove_emoji,
            remove_urls,
            remove_hashtags, 
            remove_users,
            normalize_quotes,
            fix_paragraphs,
            normalize_spaces
        )
        self.skip_substrings = processor_config.get("skip_substrings", [])
        self.rm_substrings = processor_config.get("rm_substrings", [])
        self.rm_substrings.sort(key=len, reverse=True)

    def __call__(self, text):
        if not text:
            return ""
        if self._should_skip(text):
            return ""
        text = self._remove_bad_text(text)
        for step in self.pipeline:
            text = step(text)
            if not text:
                return ""
        if len(text.strip()) < 10:
            return ""
        return text.strip()

    def _should_skip(self, text):
        text_lower = text.lower()
        return any(ss.lower() in text_lower for ss in self.skip_substrings)

    def _remove_bad_text(self, text):
        for ss in self.rm_substrings:
            text = text.replace(ss, " ")
        return text