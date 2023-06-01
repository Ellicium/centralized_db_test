from fastapi import FastAPI, Request, Form, Response

from .routers import supplier_routes

app = FastAPI(debug=True)

app.include_router(supplier_routes.router)


#################################################################
