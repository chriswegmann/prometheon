
import sqlite3

# NB: class User is not a resource, but a helper. The API does not deal with
# this class directly
# A model is an internal representation of an object
# A resource is what the API interacts with

class UserModel:
    def __init__(self, _id, username, password):
        self.id = _id
        self.username = username
        self.password = password

    @classmethod
    def find_by_username(cls, username):
        pass

    @classmethod
    def find_by_id(cls, _id):
        pass
