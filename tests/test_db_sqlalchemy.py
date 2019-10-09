import sqlite3

import pytest
import sqlalchemy
from databases import Database
from sqlalchemy.ext.declarative import declarative_base

from fastapi_users.db.sqlalchemy import BaseUser, SQLAlchemyUserDatabase


@pytest.fixture
async def sqlalchemy_user_db() -> SQLAlchemyUserDatabase:
    Base = declarative_base()

    class User(BaseUser, Base):
        pass

    DATABASE_URL = "sqlite:///./test.db"
    database = Database(DATABASE_URL)

    engine = sqlalchemy.create_engine(
        DATABASE_URL, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)

    await database.connect()

    yield SQLAlchemyUserDatabase(database, User.__table__)

    Base.metadata.drop_all(engine)


@pytest.mark.asyncio
async def test_queries(user, sqlalchemy_user_db):
    # Create
    user_db = await sqlalchemy_user_db.create(user)
    assert user_db.id is not None
    assert user_db.is_active is True
    assert user_db.is_superuser is False
    assert user_db.email == user.email

    # Update
    user_db.is_superuser = True
    await sqlalchemy_user_db.update(user_db)

    # Get by id
    id_user = await sqlalchemy_user_db.get(user.id)
    assert id_user.id == user_db.id
    assert id_user.is_superuser is True

    # Get by email
    email_user = await sqlalchemy_user_db.get_by_email(user.email)
    assert email_user.id == user_db.id

    # List
    users = await sqlalchemy_user_db.list()
    assert len(users) == 1
    first_user = users[0]
    assert first_user.id == user_db.id

    # Exception when inserting existing email
    with pytest.raises(sqlite3.IntegrityError):
        await sqlalchemy_user_db.create(user)

    # Unknown user
    unknown_user = await sqlalchemy_user_db.get_by_email("lancelot@camelot.bt")
    assert unknown_user is None
