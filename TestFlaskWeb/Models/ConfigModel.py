class ConfigModel:
    Storage_account_name = ''
    Storage_account_key = ''
    Storage_file_system = ''
    Storage_file_path = ''
    Raw_data_dir = ''
    Mysql_host = ''
    Mysql_user = ''
    Mysql_password = ''
    Mysql_ssl_ca = ''
    def __init__(self, storage_account_name ,storage_account_key ,storage_file_system ,storage_file_path ,raw_data_dir ,mysql_host ,mysql_user ,mysql_password ,mysql_ssl_ca):
        self.Storage_account_name = storage_account_name
        self.Storage_account_key = storage_account_key
        self.Storage_file_system = storage_file_system
        self.Storage_file_path = storage_file_path
        self.Raw_data_dir = raw_data_dir
        self.Mysql_host = mysql_host
        self.Mysql_user = mysql_user
        self.Mysql_password = mysql_password
        self.Mysql_ssl_ca = mysql_ssl_ca