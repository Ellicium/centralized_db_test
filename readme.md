
## server start command

gunicorn -w 2 -k uvicorn.workers.UvicornWorker app.main:app -t 600 --reload
