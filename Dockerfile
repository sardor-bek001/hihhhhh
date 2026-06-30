# Python-slim bazaviy rasmidan foydalanamiz
FROM python:3.12-slim

# Terminalda buffering-ni o'chirib qo'yamiz (loglarni darhol ko'rish uchun)
ENV PYTHONUNBUFFERED=1

# Tizim paketlarini yangilaymiz va FFmpeg o'rnatamiz
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ishchi papkani yaratamiz
WORKDIR /app

# Avval faqat requirements.txt faylini ko'chirib olib, kutubxonalarni o'rnatamiz (Docker cache-dan foydalanish uchun)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Loyihaning qolgan barcha fayllarini ko'chiramiz
COPY . .

# Botni ishga tushiramiz
CMD ["python", "main.py"]
