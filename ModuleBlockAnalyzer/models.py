from ModuleBlockAnalyzer import db


class LAST_PROCECCED_BLOCK_NUMBER(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    block_number = db.Column(db.Integer)


class UserWallet(db.Model):
    __bind_key__ = 'outer_CENTRAL_DB'
    __tablename__ = 'userwallet'

    id = db.Column(db.Integer, primary_key=True)
    address = db.Column(db.String(255), index=True, nullable=False, unique=True)
    private_key = db.Column(db.String(255), unique=True)
    currency = db.Column(db.String(50), nullable=False)


class TRANSACTION_RCW(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    state = db.Column(db.String(20), default='Pending')
    block_number = db.Column(db.Integer)
    sender_address = db.Column(db.String(42))
    receiver_address = db.Column(db.String(42))
    value = db.Column(db.String(255))
    gas = db.Column(db.BigInteger)
    gasPrice = db.Column(db.BigInteger)
    total_gas_cost = db.Column(db.String(255))
    nonce = db.Column(db.Integer)
    is_token_sending = db.Column(db.Boolean, default=False)
    token_recipient = db.Column(db.String(42))
    token_value = db.Column(db.String(255))
    token_symbol = db.Column(db.String(20))

    def __init__(self, block_number, sender_address, receiver_address, value, gas, gasPrice, total_gas_cost, nonce,
                 is_token_sending=False, token_recipient=None, token_value=None, token_symbol=None, state='Pending'):
        self.state = state
        self.block_number = block_number
        self.sender_address = sender_address
        self.receiver_address = receiver_address
        self.value = value
        self.gas = gas
        self.gasPrice = gasPrice
        self.total_gas_cost = total_gas_cost
        self.nonce = nonce
        self.is_token_sending = is_token_sending
        self.token_recipient = token_recipient
        self.token_value = token_value
        self.token_symbol = token_symbol

    def __repr__(self):
        return f'<PendingTransaction {self.id}>'
