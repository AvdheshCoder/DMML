from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TIMESTAMP, text
from sqlalchemy import ForeignKey

Base = declarative_base()


class FollowersActive(Base):

    __tablename__ = 'followers_active'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_profile_api = Column(Integer, ForeignKey('profiles_api.id'), nullable=False)
    id_profile_users = Column(Integer, ForeignKey('profiles_users.id'), nullable=False)
    #time


class ProfilesApi(Base):

    __tablename__ = 'profiles_api'

    id = Column(Integer, primary_key=True, autoincrement=True)
    twitter_handle = Column(String(100), unique=True,  nullable=False)
    key_consumer = Column(String(100), nullable=False)
    secret_consumer = Column(String(100), nullable=False)
    key_access = Column(String(100), nullable=False)
    secret_access = Column(String(100), nullable=False)


class ProfilesUsers(Base):

    __tablename__ = 'profiles_users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, primary_key=True)


class AccessTokens(Base):

    __tablename__ = 'access_tokens'

    id = Column(Integer, primary_key=True, autoincrement=True)
    token = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False) #state can be valid or invalid
    ts_create = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    ts_update = Column(TIMESTAMP, server_onupdate=text('CURRENT_TIMESTAMP'))


class JobDetails(Base):

    __tablename__ = 'job_details'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(100), nullable=False)
    status = Column(String(100), nullable=False)
    ts_create = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    ts_update = Column(TIMESTAMP, server_onupdate=text('CURRENT_TIMESTAMP'))
    entries = Column(Integer, nullable=False)
    failed = Column(Integer, nullable=False)
    completed = Column(Integer, nullable=False)


class FollowingStatus(Base):

    __tablename__ = 'following_status'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_profile_api = Column(Integer, ForeignKey('profiles_api.id'), nullable=True)
    id_profile_users = Column(Integer, ForeignKey('profiles_users.id'), nullable=False)
    status = Column(String(100), nullable=False)
    job_name = Column(String(100), nullable=False)
    ts_create = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    ts_update = Column(TIMESTAMP, server_onupdate=text('CURRENT_TIMESTAMP'))
    response_string = Column(String(1000), nullable=True)


    def __init__(self, id_profile_api, id_profile_users,status,job_name):
        self.id_profile_api = id_profile_api
        self.id_profile_users = id_profile_users
        self.status = status
        self.job_name = job_name

