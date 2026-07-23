"""initial

Revision ID: d1e6fe5042de
Revises: cacf1e338baa
Create Date: 2026-07-23 00:09:21.824602

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd1e6fe5042de'
down_revision: Union[str, Sequence[str], None] = 'cacf1e338baa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
