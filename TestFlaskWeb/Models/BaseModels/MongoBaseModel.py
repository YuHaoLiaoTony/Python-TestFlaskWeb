class MongoBaseModel():
    __Key__ = '_id'
    _id  = ''
    def __init__(self):
        self._id = ''

    @property
    def Id(self):
        return str(self._id)

    @Id.setter
    def Id(self, value):
        self._id = value