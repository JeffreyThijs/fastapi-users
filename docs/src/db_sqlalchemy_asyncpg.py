from fastapi import FastAPI
from fastapi_users import models
from fastapi_users.db import SQLAlchemyBaseUserTable, SQLAlchemyAsyncPGUserDatabase
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine


class User(models.BaseUser):
    pass


class UserCreate(models.BaseUserCreate):
    pass


class UserUpdate(User, models.BaseUserUpdate):
    pass


class UserDB(User, models.BaseUserDB):
    pass


DATABASE_URL = "sqlite:///./test.db"
# for postgres use:
# DATABASE_URL = "postgresql+asyncpg://username:password@127.0.0.1:5432"

Base = declarative_base()


class UserTable(Base, SQLAlchemyBaseUserTable):
    pass


engine: AsyncEngine = create_async_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

Base.metadata.create_all(engine)

users = UserTable.__table__
user_db = SQLAlchemyAsyncPGUserDatabase(UserDB, engine, users)

app = FastAPI()