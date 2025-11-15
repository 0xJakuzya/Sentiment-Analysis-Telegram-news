# Sentiment Analysis для Telegram новостей

Проект для анализа тональности новостей из Telegram каналов с использованием машинного обучения.

## Будущие Возможности

- Скрапинг сообщений из Telegram каналов
- Предобработка текста (очистка HTML, нормализация, удаление URL и т.д.)
- Классификация тональности (позитив/негатив/нейтрал) с использованием BERT моделей
- Сохранение результатов в MongoDB и JSON
- Docker поддержка для легкого развертывания

## Структура проекта
```
Sentiment-Analysis-Telegram-news/
├── config/
|   ├── channel.json      # Список телеграмм-каналов 
│   ├── mongo_config.json # Конфигурация MongoDB
|   ├── preprocessing.json # Конфигурация Предообработки 
├── src/
│   ├── telegram_scraping # Модуль скрапинга Telegram
│   ├── preprocessing     # Предобработка текста
│   ├── embedding         # Создание эмбеддингов
│   ├── classification    # Классификация тональности
│   ├── pipeline          # Пайплайн обработки
│   └── mongo             # Работа с MongoDB
├── scripts/
│   ├── config.py          # Скрипт для вызова конфигураций
├── data/                  # Данные (raw, processed, labeled)
├── sessions/              # Telegram сессии
├── main.py                # Главный скрипт
├── requirements.txt       # Зависимости
├── Dockerfile             # Docker образ
└── docker-compose.yml     # Docker Compose конфигурация
```

