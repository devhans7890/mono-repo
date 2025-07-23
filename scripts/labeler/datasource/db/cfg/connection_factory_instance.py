from typing import List
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

from express_utils.express_exception_utils import ExpressExceptionUtils


Base = declarative_base()


class ConnectionFactoryInstance:
    _instance = None
    _lock = threading.Lock()  # Lock 초기화

    def __new__(cls, identifier: str, json_element: list):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ConnectionFactoryInstance, cls).__new__(cls)
                    cls._instance.__init(identifier, json_element)
        return cls._instance

    def __init(self, identifier: str, json_element: list):
        self.engine = None
        self.session = None
        try:
            for obj in json_element:
                pool_id = obj.get("id", "")
                if pool_id != identifier:
                    continue
                db_url = None
                if obj.get("enable"):
                    db_url = self._generate_sqlalchemy_url(
                        dialect=obj.get("dialect", "mysql"),
                        driver=obj.get("driver", "pymysql"),
                        user_name=obj.get("username", "dfinder"),
                        password=obj.get("password", ""),
                        host=obj.get("host"),
                        database=obj.get("database", "dfinder")
                    )
                self.engine = create_engine(
                    db_url,
                    pool_size=obj.get("pool_size"),
                    max_overflow=obj.get("max_overflow",10),
                    pool_timeout=obj.get("pool_timeout",28800),
                    pool_pre_ping=True,
                    pool_recycle=obj.get("pool_recycle", 60)
                )

                self.session = sessionmaker(bind=self.engine)
        except Exception as e:
            print(
                f"Error in ConnectionFactoryInstance initialization: {ExpressExceptionUtils.get_stack_trace(e)}"
            )

    @staticmethod
    def get_instance():
        if ConnectionFactoryInstance._instance is None:
            raise Exception("ConnectionFactoryInstance is not initialized yet.")
        return ConnectionFactoryInstance._instance

    def _generate_sqlalchemy_url(
            self,
            dialect: str,
            driver: str,
            user_name: str,
            password: str,
            host: List[str],
            database: str
    ) -> str:
        if not host:
            print("Host is empty. Please check the alchemy host in the pool.yml file.")
            raise ValueError("Host is empty")
        if len(host) >= 1:
            print(f"Multiple hosts are not supported. Only the first one({host[0]}) will be used.")
        host = host[0]
        url = f"{dialect}+{driver}://{user_name}:{password}@{host}/{database}"
        return url

    def get_session(self):
        if self.session is None:
            print("Session factory is not initialized")
            raise Exception("Session factory is not initialized")
        return self.session()
