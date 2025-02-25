from sqlalchemy.orm import Session
from core.database import SessionLocal  # Import SessionLocal from core
from typing import Annotated
from fastapi import Depends

def get_db():
    db= SessionLocal()
    try:
        yield db
    finally:
        db.close()


