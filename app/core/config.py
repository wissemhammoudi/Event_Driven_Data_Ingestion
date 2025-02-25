from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database connection details
    USERNAME: str
    PASSWORD: str
    KAFKA_BROKER: str
    URL: str
    
    class Config:
        # Specify the path to your .env file
        env_file = "./core/.env"
