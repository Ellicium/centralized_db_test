
## server start command
### Linux
gunicorn -w 2 -k uvicorn.workers.UvicornWorker app.main:app -t 600 --reload
### Windows
uvicorn app.main:app --port 8080 --workers 2 --reload
