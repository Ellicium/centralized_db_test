from fastapi import FastAPI, Request, Form, Response

from .routers import supplier_routes,update_routes

app = FastAPI(debug=True)

app.include_router(supplier_routes.router)
app.include_router(update_routes.router)


#################################################################
