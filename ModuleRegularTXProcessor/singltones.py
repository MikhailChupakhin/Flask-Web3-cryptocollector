from decouple import config
from flask_sqlalchemy import SQLAlchemy
from web3 import Web3
from web3.middleware import geth_poa_middleware


class Web3Singleton:
    _instance = None
    def __new__(cls):
        if not cls._instance:
            provider_url = config('PROVIDER_URL')
            cls._instance = Web3(Web3.HTTPProvider(provider_url))
            cls._instance.middleware_onion.inject(geth_poa_middleware, layer=0)
        return cls._instance


class DatabaseSingleton:
    _db_instance = None

    def __new__(cls, app=None):
        if cls._db_instance is None:
            cls._db_instance = super(DatabaseSingleton, cls).__new__(cls)
            if app is not None:
                cls._db_instance.db = cls._create_db_instance(app)
        return cls._db_instance

    @staticmethod
    def _create_db_instance(app):
        db = SQLAlchemy()
        if 'sqlalchemy' not in app.extensions:
            db.init_app(app)
        else:
            db = app.extensions['sqlalchemy'].db

        return db