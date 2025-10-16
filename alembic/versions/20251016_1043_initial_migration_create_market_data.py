"""Initial migration - create market_data table

Revision ID: 001
Revises:
Create Date: 2025-10-16 10:43:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create market_data table
    op.create_table(
        'market_data',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('ticker', sa.String(length=10), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('open', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('high', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('low', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('close', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('volume', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('ticker', 'date', name='uq_ticker_date')
    )

    # Create indexes
    op.create_index('ix_ticker_date', 'market_data', ['ticker', 'date'], unique=False)
    op.create_index(op.f('ix_market_data_ticker'), 'market_data', ['ticker'], unique=False)
    op.create_index(op.f('ix_market_data_date'), 'market_data', ['date'], unique=False)


def downgrade() -> None:
    # Drop indexes
    op.drop_index(op.f('ix_market_data_date'), table_name='market_data')
    op.drop_index(op.f('ix_market_data_ticker'), table_name='market_data')
    op.drop_index('ix_ticker_date', table_name='market_data')

    # Drop table
    op.drop_table('market_data')
