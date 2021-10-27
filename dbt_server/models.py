from sqlalchemy import Column, String

from .database import Base


class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(String, primary_key=True, index=True)
    state = Column(String, default='pending')

    command = Column(String)
    log_path = Column(String)
