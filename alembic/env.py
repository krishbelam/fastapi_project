import os
from logging.config import fileConfig

# ========================================================================================================
#                       To set the data for the sync db migrations
#                           (Uncomment if working with sync)
# ========================================================================================================
# from sqlalchemy import engine_from_config
# from sqlalchemy import pool
# from alembic import context
# ========================================================================================================


# ========================================================================================================
#                       To set the data for the async db migrations
#                               (Comment if working with sync)
# ========================================================================================================
import asyncio
from sqlalchemy.ext.asyncio import async_engine_from_config, AsyncConnection
from alembic import context
# ========================================================================================================


# ========================================================================================================
#                                   Use Absolute imports
# ========================================================================================================
from fastapi_app.core.config import settings
from fastapi_app.models.orm import *   # imports all mapped classes
# ========================================================================================================


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
# target_metadata = None

# ========================================================================================================
#                       To set the database url from the settings in configs
# ========================================================================================================
config.set_main_option("sqlalchemy.url", settings.DB_URL)

target_metadata = Base.metadata


# ========================================================================================================

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


# ======================================================================================================
#           Commented: Using alembic with async so NEW setup done in the bottom
# ======================================================================================================
'''
def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
'''


# ======================================================================================================

# You created an async_engine but Alembic's default run_migrations_online() uses sync connect(). Fix — use asyncio.run with the async engine:


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online():
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


asyncio.run(run_migrations_online())

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
