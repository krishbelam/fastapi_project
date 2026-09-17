"""create users table

Revision ID: cacf1e338baa
Revises: 89cc93b7e46d
Create Date: 2026-07-23 00:07:06.155669

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cacf1e338baa'
down_revision: Union[str, Sequence[str], None] = '89cc93b7e46d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
