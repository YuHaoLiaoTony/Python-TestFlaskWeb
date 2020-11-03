class Monitor:

	def __init__(self, mysql_host, mysql_user, mysql_password, mysql_ssl_ca, \
		bu=None, analysis_theme=None, \
		storage_account_name=None, storage_account_key=None, file_system_name=None,\
		dtl_dir    = None, \
		blob_output_dir = None, \
		start_date=None, end_date=None):

		self.mysql_host            = mysql_host
		self.mysql_user            = mysql_user
		self.mysql_password        = mysql_password
		self.mysql_ssl_ca          = mysql_ssl_ca

		self.bu                    = bu
		self.analysis_theme        = analysis_theme

		self.storage_account_name  = storage_account_name
		self.storage_account_key   = storage_account_key
		self.file_system_name      = file_system_name

		self.dtl_dir               = dtl_dir
		self.blob_output_dir       = blob_output_dir

		self.start_date            = start_date
		self.end_date              = end_date


		# Default argument
		if self.bu is None:
			self.bu = "CCF"
		else: 
			self.bu = self.bu

		if self.analysis_theme is None:
			self.analysis_theme  = "approve"
		else: 
			self.analysis_theme   = self.analysis_theme

		if self.storage_account_name is None:
			self.storage_account_name = "momostoragev2"
		else: 
			self.storage_account_name = self.storage_account_name

		if self.storage_account_key is None:
			self.storage_account_key  = "JZiPNJrdrIadjy2xswwPqu2NWHBHYOQ7x8xwH2VGTuOuic9I8OVkHMPefDNSxHgCpR88VK1qnPMC5Man90COyw=="
		else: 
			self.storage_account_key   = self.storage_account_key

		if self.file_system_name is None:
			self.file_system_name  = "uatdatalake"
		else: 
			self.file_system_name   = self.file_system_name

		if self.dtl_dir is None:
			self.dtl_dir='10997/20200928/ccf_acs_app_dtl.out'
		else: 
			self.dtl_dir = self.dtl_dir

		today  = datetime.today().strftime('%Y%m%d')

		if self.blob_output_dir is None:
			self.blob_output_dir = f'/ITRI/Report/Monitor/{today}/'
		else: 
			self.blob_output_dir = self.blob_output_dir


		start_date, end_date = time_mask(start_date=self.start_date, end_date=self.end_date)
		self.start_date = start_date
		self.end_date   = end_date


	def Reproting(self): 

		try:
			con = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, ssl={'ssl': {'ssl-ca': mysql_ssl_ca}}, charset='utf8')
			print('Connected to DB: {}'.format(mysql_host))
			cursor = con.cursor()

			sql = f"SELECT * FROM acs.prediction_log WHERE APLY_DT BETWEEN {self.start_date.strftime('%Y%m%d')} AND {self.end_date.strftime('%Y%m%d')}"
			df_check = pd.read_sql(sql, con=con) 
			con.close()
		except Exception as e:
			print('Error: {}'.format(str(e)))
			sys.exit(1)

		try:  
			file_list            = []
			file_type            = '.csv'
			#storage_account_key  = "ygM6jtNW7+3zu5JjY+FWFbclf78OR3l9bZpeMeZKPy+73VX93q7OD3qeQqYBgWCkUu3cTIxlLergvXMBaj4HyQ=="

			service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
			"https", self.storage_account_name), credential=self.storage_account_key)

			# Get File System List
			filesystem_client = service_client.get_file_system_client(file_system=self.file_system_name)

		except Exception as e:
			print(e)

		# create directory
		try:
			filesystem_client.create_directory(self.blob_output_dir)

		except Exception as e:
			print(e) 


		self.start_date = self.start_date.strftime('%Y%m%d')
		self.end_date   = self.end_date.strftime('%Y%m%d')
         
		df_prediction = monitoring_prediction(df_check, self.bu, self.analysis_theme, \
		start_date=self.start_date, end_date=self.end_date)   

		df_error_rate = monitoring_error_rate(df_check, self.bu, self.analysis_theme, \
		dtl_dir    = self.dtl_dir, \
		storage_account_name = self.storage_account_name, \
		storage_account_key  = self.storage_account_key, \
		file_system_name     = self.file_system_name, \
		start_date=self.start_date, end_date=self.end_date)         

		today  = datetime.today().strftime('%Y%m%d')

		if df_prediction is None:
			df_prediction = pd.DataFrame()
		else: pass
		if df_error_rate is None:
			df_error_rate = pd.DataFrame()
		else: pass



		if type(self.blob_output_dir) == list:

			for output_dir in self.blob_output_dir:
				output_file = f'{self.bu}_{self.analysis_theme}_{today}.xlsx'
				writer = pd.ExcelWriter('/local_disk0/tmp/'+output_file, engine='xlsxwriter')

				# Convert the dataframe to an XlsxWriter Excel object.
				df_prediction.to_excel( writer, sheet_name='Drift_Report',  index=True)
				if df_error_rate.empty == False:
					df_error_rate.to_excel( writer, sheet_name='Error_Rate_Report',  index=True)
				else: pass
				# Close the Pandas Excel writer and output the Excel file.
				writer.close()

				# Copy File From Local to DBFS
				shutil.move('/local_disk0/tmp/'+output_file, '/dbfs'+'/user/'+output_file)


				with open('/dbfs'+'/user/'+output_file, 'rb') as f:
					t = f.read()
					file = filesystem_client.create_file(output_dir+output_file)
					file.append_data(t, offset=0, length=len(t))
					file.flush_data(len(t))

		else:
			output_file = f'{self.bu}_{self.analysis_theme}_{today}.xlsx'
			writer = pd.ExcelWriter('/local_disk0/tmp/'+output_file, engine='xlsxwriter')

			# Convert the dataframe to an XlsxWriter Excel object.
			df_prediction.to_excel( writer, sheet_name='Drift_Report',  index=True)
			if df_error_rate.empty == False:
					df_error_rate.to_excel( writer, sheet_name='Error_Rate_Report',  index=True)
			else: pass

			# Close the Pandas Excel writer and output the Excel file.
			writer.close()

			# Copy File From Local to DBFS
			shutil.move('/local_disk0/tmp/'+output_file, '/dbfs'+'/user/Monitor/'+output_file)


			with open('/dbfs'+'/user/Monitor/'+output_file, 'rb') as f:
			  t = f.read()
			  file = filesystem_client.create_file(self.blob_output_dir+output_file)
			  file.append_data(t, offset=0, length=len(t))
			  file.flush_data(len(t))

		print('Report Saved!')


		return df_prediction, df_error_rate


# COMMAND ----------