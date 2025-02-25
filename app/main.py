from fastapi import FastAPI

import routers.topic, routers.producer,routers.consumer,routers.workspace,routers.sources,routers.destination,routers.connection



app = FastAPI()

# Include Kafka routes
app.include_router(routers.topic.router)
app.include_router(routers.producer.router)
app.include_router(routers.consumer.router)
app.include_router(routers.workspace.router)
app.include_router(routers.sources.router)
app.include_router(routers.destination.router)
app.include_router(routers.connection.router)







@app.get("/")
def root():
    return {"message": "Kafka FastAPI service is running!"}
