"""Retention rules pending status.

Revision ID: 377cfbfccf75
Revises: b92fe567be5b
Create Date: 2022-10-06 11:40:29.912743

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "377cfbfccf75"
down_revision = "b92fe567be5b"
branch_labels = None
depends_on = None

# Name of the status enum type
enum_name = "workspaceretentionrulestatus"
tmp_name = "workspaceretentionrulestatus_tmp"


def upgrade():
    """Upgrade to 377cfbfccf75 revision."""
    # Rename the old enum type to a temporary name
    op.execute(f"ALTER TYPE {enum_name} RENAME TO {tmp_name};")
    # Create the new enum type adding the `pending` status
    sa.Enum(
        "created",
        "active",
        "inactive",
        "applied",
        "pending",
        name=enum_name,
    ).create(op.get_bind())
    # Convert the status column in the retention rules table to the new type
    op.execute(
        "ALTER TABLE __reana.workspace_retention_rule "
        f"ALTER COLUMN status TYPE {enum_name} "
        f"USING status::text::{enum_name};"
    )
    # Convert the action column in the audit log table to the new type
    op.execute(
        "ALTER TABLE __reana.workspace_retention_audit_log "
        f"ALTER COLUMN action TYPE {enum_name} "
        f"USING action::text::{enum_name};"
    )
    # Delete the old enum type
    sa.Enum(name=tmp_name).drop(op.get_bind())


def downgrade():
    """Downgrade to b92fe567be5b revision."""
    op.execute(f"ALTER TYPE {enum_name} RENAME TO {tmp_name};")
    sa.Enum(
        "created",
        "active",
        "inactive",
        "applied",
        name=enum_name,
    ).create(op.get_bind())
    # Change the status of all the pending rules to `active`
    op.execute(
        "UPDATE __reana.workspace_retention_rule "
        "SET status='active' WHERE status='pending';"
    )
    op.execute(
        "ALTER TABLE __reana.workspace_retention_rule "
        f"ALTER COLUMN status TYPE {enum_name} "
        f"USING status::text::{enum_name};"
    )
    # Delete all the audit log entries that have `pending` as action
    op.execute(
        "DELETE FROM __reana.workspace_retention_audit_log WHERE action='pending';"
    )
    op.execute(
        "ALTER TABLE __reana.workspace_retention_audit_log "
        f"ALTER COLUMN action TYPE {enum_name} "
        f"USING action::text::{enum_name};"
    )
    sa.Enum(name=tmp_name).drop(op.get_bind())