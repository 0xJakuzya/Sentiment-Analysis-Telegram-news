import os
import json
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDBClient:

    def __init__(self, uri=None, db_name=None, collection_name=None):

        self.uri = uri or os.getenv("MONGODB_URI")
        self.db_name = db_name or os.getenv("MONGODB_DB")
        self.collection_name = collection_name or os.getenv("MONGODB_COLLECTION")
        self._client = None
        self._collection = None
        self._connect()

    #подключение к mongodb
    def _connect(self):

        try:
            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=2000)
            self._client.server_info()  
            self._collection = self._client[self.db_name][self.collection_name]
            print(f"Подключено к MongoDB: {self.db_name}/{self.collection_name}")

        except Exception as e:
            print(f"Не удалось подключиться к MongoDB: {e}")
            self._client = None
            self._collection = None

    @property
    def available(self):
        return self._collection is not None

    #cохранение json-данные в папку data/raw в формате messages_{timestamp}.json
    def _save_to_raw_files(self, messages):
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = f"data/raw/messages_{timestamp}.json"
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            json_messages = []
            for msg in messages:
                json_msg = dict(msg)
                if isinstance(json_msg.get("date"), datetime):
                    json_msg["date"] = json_msg["date"].isoformat()
                if isinstance(json_msg.get("saved_at"), datetime):
                    json_msg["saved_at"] = json_msg["saved_at"].isoformat()
                json_messages.append(json_msg)
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(json_messages, f, ensure_ascii=False, indent=2)
            print(f"Сохранено {len(json_messages)} сообщений в JSON: {filepath}")
        except Exception as e:
            print(f"Ошибка при сохранении в JSON: {e}")

    #сохранение данных в mongodb и в data/raw(data lake)
    def save_messages(self, messages):

        if not messages:
            return 0

        normalized = []
        for message in messages:
            doc = dict(message)
            doc.setdefault("saved_at", datetime.utcnow())
            normalized.append(doc)

        # data/raw
        self._save_to_raw_files(normalized)

        # mongodb
        if self.available:
            try:
                result = self._collection.insert_many(normalized, ordered=False)
                print(f"Сохранено {len(result.inserted_ids)} сообщений в MongoDB")
                return len(result.inserted_ids)
            except PyMongoError as e:
                print(f"Ошибка при сохранении в MongoDB: {e}")
                return 0
        
        print("MongoDB недоступна. Сообщения не сохранены.")
        return 0

    def close(self) -> None:
        if self._client:
            self._client.close()
            print("Соединение с MongoDB закрыто")