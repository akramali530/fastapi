FROM python:3.11-alpine
COPY . .
RUN pip install uvicorn
RUN pip install fastapi
CMD ["uvicorn", "main:app", "--host", "0.0.0.0","--port", "80"]