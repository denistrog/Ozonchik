FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY catalog.py index.html legacy_ozonchik.html /app/
RUN mkdir -p /data
EXPOSE 8765
CMD ["python", "catalog.py", "serve"]
