from Models.BaseModels.MongoBaseModel import MongoBaseModel

class ProductModel(MongoBaseModel):
    Name = ''
    Y = ''
    def __all__(self):
        
        attribs = self.__dict__
        all = dir(self.__class__)
        for name in dir(self.__class__):
            obj = getattr(self.__class__, name)
            if isinstance(obj, property):
               val = obj.__get__(self, self.__class__)
               attribs[name]= val
               
        keys = [i for i in self.__class__.__dict__.keys() if i[:1] != '_' and i not in attribs.keys()]

        for key in keys:
            attribs[key]= ''
               
        attribs.pop(self.__Key__)

        return attribs