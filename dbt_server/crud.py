from sqlalchemy.orm import Session
from dbt_server.database import SessionLocal

from . import models, schemas


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_task(db: Session, task_id: str):
    return db.query(models.Task).filter(models.Task.task_id == task_id).first()


def create_task(db: Session, task: schemas.Task):
    db_task = models.Task(**task.dict())
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


def set_task_running(db: Session, task: schemas.Task):
    db_task = get_task(db, task.task_id)
    db_task.state = models.TaskState.RUNNING
    db.commit()
    db.refresh(db_task)
    return db_task


def set_task_done(db: Session, task: schemas.Task):
    db_task = get_task(db, task.task_id)
    db_task.state = models.TaskState.FINISHED
    db.commit()
    db.refresh(db_task)
    return db_task


def set_task_errored(db: Session, task: schemas.Task, error: str):
    db_task = get_task(db, task.task_id)
    db_task.state = models.TaskState.ERROR
    db_task.error = error
    db.commit()
    db.refresh(db_task)
    return db_task
