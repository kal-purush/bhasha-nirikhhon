"""fix ENUM useractiontype bug

Revision ID: c7238487f08d
Revises: 5492f53d1fa5
Create Date: 2024-05-07 11:04:38.575959

"""

from typing import Sequence, Union

from alembic import op, context


# revision identifiers, used by Alembic.
revision: str = "c7238487f08d"
down_revision: Union[str, None] = "5492f53d1fa5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

old_options = ("SUBMIT", "ACCEPT", "SIGNED", "CREATE")

new_options = ("SUBMIT", "ACCEPT", "SIGN", "CREATE")


def upgrade() -> None:
    if "sqlite" not in context.get_context().dialect.name:
        op.execute("ALTER TYPE useractiontype RENAME TO useractiontype_old")
        op.execute(f"CREATE TYPE useractiontype AS ENUM{new_options}")
        op.execute(
            "ALTER TABLE user_action ALTER COLUMN action_type TYPE useractiontype USING action_type::text::useractiontype"
        )
        op.execute("DROP TYPE useractiontype_old")


def downgrade() -> None:
    if "sqlite" not in context.get_context().dialect.name:
        op.execute("ALTER TYPE useractiontype RENAME TO useractiontype_old")
        op.execute(f"CREATE TYPE useractiontype AS ENUM{old_options}")
        op.execute(
            "ALTER TABLE user_action ALTER COLUMN action_type TYPE useractiontype USING action_type::text::useractiontype"
        )
        op.execute("DROP TYPE useractiontype_old")