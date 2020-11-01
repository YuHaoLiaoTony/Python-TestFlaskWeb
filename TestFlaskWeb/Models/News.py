class News:
    __Key__= 'Id'
    Id = 0
    DiasterId = 0
    Title = ''
    Content = ''
    CreateDate = ''
    CreateUser = ''
    UpdateDate = ''
    UpdateUser = ''
    def __init__(self,id,diasterId,title,content,createDate,createUser,updateDate,updateUser):
        self.Id = id
        self.DiasterId = diasterId
        self.Title = title
        self.Content = content
        self.CreateDate = createDate
        self.CreateUser = createUser
        self.UpdateDate = updateDate
        self.UpdateUser = updateUser
    
        
