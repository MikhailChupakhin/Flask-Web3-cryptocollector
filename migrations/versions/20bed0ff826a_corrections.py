"""Corrections

Revision ID: 20bed0ff826a
Revises: 755218317f07
Create Date: 2023-12-11 20:58:44.504286

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20bed0ff826a'
down_revision = '755218317f07'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('pending_transaction', schema=None) as batch_op:
        batch_op.alter_column('gas',
               existing_type=sa.INTEGER(),
               type_=sa.BigInteger(),
               existing_nullable=True)

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('pending_transaction', schema=None) as batch_op:
        batch_op.alter_column('gas',
               existing_type=sa.BigInteger(),
               type_=sa.INTEGER(),
               existing_nullable=True)

    # ### end Alembic commands ###
