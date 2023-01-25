from sqlalchemy import Column, String
from enum import Enum
from .database import Base


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"


class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(String, primary_key=True, index=True)
    state = Column(String, default=TaskState.PENDING)
    error = Column(String, default=None)

    command = Column(String)
    log_path = Column(String)
