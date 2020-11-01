class Text:
    __Key__= 'Id'
    Id = 0
    Topic = ''
    PhoneNumber = ''
    Content = ''
    SendTime = ''
    SendResult = ''
    def __init__(self,id,topic,phoneNumber,content,sendTime,sendResult):
        self.Id = id
        self.Topic = topic
        self.PhoneNumber = phoneNumber
        self.Content = content
        self.SendTime = sendTime
        self.SendResult = sendResult
    
        
