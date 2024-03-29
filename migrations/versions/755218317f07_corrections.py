"""Corrections

Revision ID: 755218317f07
Revises: 2ca4c2b78cc9
Create Date: 2023-12-11 20:31:36.628556

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '755218317f07'
down_revision = '2ca4c2b78cc9'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('pending_transaction', schema=None) as batch_op:
        batch_op.add_column(sa.Column('is_token_sending', sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column('token_recipient', sa.String(length=42), nullable=True))
        batch_op.add_column(sa.Column('token_value', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('token_symbol', sa.String(length=20), nullable=True))
        batch_op.drop_column('status')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('pending_transaction', schema=None) as batch_op:
        batch_op.add_column(sa.Column('status', sa.VARCHAR(length=20), autoincrement=False, nullable=True))
        batch_op.drop_column('token_symbol')
        batch_op.drop_column('token_value')
        batch_op.drop_column('token_recipient')
        batch_op.drop_column('is_token_sending')

    # ### end Alembic commands ###
