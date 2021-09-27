from typing import List, Optional

from pydantic import BaseModel


class Task(BaseModel):
    task_id: str
    state: str
    command: Optional[str] = None
    log_path: Optional[str] = None

    class Config:
        orm_mode = True
