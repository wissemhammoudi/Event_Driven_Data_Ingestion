from fastapi import FastAPI

import routers.topic, routers.debezium,routers.consumer,routers.sources,routers.destination,routers.connection,routers.job



app = FastAPI()

# Include Kafka routes
app.include_router(routers.topic.router)
app.include_router(routers.debezium.router)
app.include_router(routers.consumer.router)
app.include_router(routers.sources.router)
app.include_router(routers.destination.router)
app.include_router(routers.connection.router)
app.include_router(routers.job.router)








@app.get("/")
def root():
    return {"message": "Kafka FastAPI service is running!"}
