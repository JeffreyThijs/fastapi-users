import sqlite3
from typing import AsyncGenerator

import pytest
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import IntegrityError

from fastapi_users.db.sqlalchemy import (
    NotSetOAuthAccountTableError,
    SQLAlchemyBaseOAuthAccountTable,
    SQLAlchemyBaseUserTable,
    SQLAlchemyAsyncPGUserDatabase,
)
from fastapi_users.password import get_password_hash
from tests.conftest import UserDB, UserDBOAuth

@pytest.fixture
async def sqlalchemy_user_db() -> AsyncGenerator[SQLAlchemyAsyncPGUserDatabase, None]:
    Base = declarative_base()

    class User(SQLAlchemyBaseUserTable, Base):
        first_name = Column(String, nullable=True)

    DATABASE_URL = "sqlite:///./test-sqlalchemy-user.db"
    engine = create_async_engine(
        DATABASE_URL, connect_args={"check_same_thread": False}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    yield SQLAlchemyAsyncPGUserDatabase(UserDB, engine, User.__table__)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        

@pytest.fixture
async def sqlalchemy_user_db_oauth() -> AsyncGenerator[SQLAlchemyAsyncPGUserDatabase, None]:
    Base = declarative_base()

    class User(SQLAlchemyBaseUserTable, Base):
        first_name = Column(String, nullable=True)

    class OAuthAccount(SQLAlchemyBaseOAuthAccountTable, Base):
        pass

    DATABASE_URL = "sqlite:///./test-sqlalchemy-user-oauth.db"
    engine = create_async_engine(
        DATABASE_URL, connect_args={"check_same_thread": False}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield SQLAlchemyAsyncPGUserDatabase(
        UserDBOAuth, engine, User.__table__, OAuthAccount.__table__
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        

@pytest.mark.asyncio
@pytest.mark.db
async def test_queries(sqlalchemy_user_db: SQLAlchemyAsyncPGUserDatabase[UserDB]):
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password=get_password_hash("guinevere"),
    )

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
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.is_superuser is True

    # Get by email
    email_user = await sqlalchemy_user_db.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id

    # Get by uppercased email
    email_user = await sqlalchemy_user_db.get_by_email("Lancelot@camelot.bt")
    assert email_user is not None
    assert email_user.id == user_db.id

    # Exception when inserting existing email
    with pytest.raises(IntegrityError):
        await sqlalchemy_user_db.create(user)

    # Exception when inserting non-nullable fields
    with pytest.raises(IntegrityError): 
        wrong_user = UserDB(hashed_password="aaa")
        await sqlalchemy_user_db.create(wrong_user)

    # Unknown user
    unknown_user = await sqlalchemy_user_db.get_by_email("galahad@camelot.bt")
    assert unknown_user is None

    # Delete user
    await sqlalchemy_user_db.delete(user)
    deleted_user = await sqlalchemy_user_db.get(user.id)
    assert deleted_user is None

    # Exception when creating/updating a OAuth user
    user_oauth = UserDBOAuth(
        email="lancelot@camelot.bt",
        hashed_password=get_password_hash("guinevere"),
    )
    with pytest.raises(NotSetOAuthAccountTableError):
        await sqlalchemy_user_db.create(user_oauth)
    with pytest.raises(NotSetOAuthAccountTableError):
        await sqlalchemy_user_db.update(user_oauth)

    # Exception when trying to get by OAuth account
    with pytest.raises(NotSetOAuthAccountTableError):
        await sqlalchemy_user_db.get_by_oauth_account("foo", "bar")


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_custom_fields(
    sqlalchemy_user_db: SQLAlchemyAsyncPGUserDatabase[UserDB],
):
    """It should output custom fields in query result."""
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password=get_password_hash("guinevere"),
        first_name="Lancelot",
    )
    await sqlalchemy_user_db.create(user)

    id_user = await sqlalchemy_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.first_name == user.first_name


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_oauth(
    sqlalchemy_user_db_oauth: SQLAlchemyAsyncPGUserDatabase[UserDBOAuth],
    oauth_account1,
    oauth_account2,
):
    user = UserDBOAuth(
        email="lancelot@camelot.bt",
        hashed_password=get_password_hash("guinevere"),
        oauth_accounts=[oauth_account1, oauth_account2],
    )

    # Create
    user_db = await sqlalchemy_user_db_oauth.create(user)
    assert user_db.id is not None
    assert hasattr(user_db, "oauth_accounts")
    assert len(user_db.oauth_accounts) == 2

    # Update
    user_db.oauth_accounts[0].access_token = "NEW_TOKEN"
    await sqlalchemy_user_db_oauth.update(user_db)

    # Get by id
    id_user = await sqlalchemy_user_db_oauth.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.oauth_accounts[0].access_token == "NEW_TOKEN"

    # Get by email
    email_user = await sqlalchemy_user_db_oauth.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id
    assert len(email_user.oauth_accounts) == 2

    # Get by OAuth account
    oauth_user = await sqlalchemy_user_db_oauth.get_by_oauth_account(
        oauth_account1.oauth_name, oauth_account1.account_id
    )
    assert oauth_user is not None
    assert oauth_user.id == user.id

    # Unknown OAuth account
    unknown_oauth_user = await sqlalchemy_user_db_oauth.get_by_oauth_account(
        "foo", "bar"
    )
    assert unknown_oauth_user is None
