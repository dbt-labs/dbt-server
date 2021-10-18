from . import models
from .database import engine
from .views import app
from .services import dbt_service

# Where... does this actually go?
# And what the heck do we do about migrations?
models.Base.metadata.create_all(bind=engine)

# TODO : This messes with stuff
dbt_service.disable_tracking()


@app.on_event("startup")
async def startup_event():
    pass


@app.on_event("shutdown")
def shutdown_event():
    pass
