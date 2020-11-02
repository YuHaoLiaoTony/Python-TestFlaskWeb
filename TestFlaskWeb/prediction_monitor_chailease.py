# Databricks notebook source
# coding: utf-8
# Authur          : Angeli C.-J. Wu
# Produce Date    : 2020/10/23
# Last Update Date: 

# COMMAND ----------

# setting - 請將DATALAKE測試環境路徑替換為正式環境路徑
storage_account_name = "momostoragev2"
storage_account_key  = "JZiPNJrdrIadjy2xswwPqu2NWHBHYOQ7x8xwH2VGTuOuic9I8OVkHMPefDNSxHgCpR88VK1qnPMC5Man90COyw=="
storage_file_system = "uatdatalake"
storage_file_path = "BI/CCF/DAY/" #這個資料夾路經應該要是BI每日上傳.out檔的路徑

# setting - 請將mysql測試環境路徑替換為正式環境路徑
raw_data_dir = '/dbfs/uatdatalake/BI/'
mysql_host = "acmysqltest01.mysql.database.azure.com"
mysql_user = "myadmin@acmysqltest01"
mysql_password = "S@lcfc8888"
mysql_ssl_ca = raw_data_dir+"BaltimoreCyberTrustRoot.crt.pem"

# COMMAND ----------

# MAGIC %md # Importing Libraries

# COMMAND ----------



# COMMAND ----------

# Python Version: '3.7.3 (default, Oct 30 2019, 07:22:34) \n[GCC 5.4.0 20160609]'
# %sh pip freeze
# Databricks notebook source

# opencensus-ext-pymysql==0.1.2
import os, re, sys, uuid, glob, shutil, pymysql
from io            import StringIO
from datetime import datetime, date, timedelta, time
#numpy==1.16.2
import numpy       as np
# pandas==0.24.2
import pandas      as pd
from pandas.tseries.offsets import DateOffset
# azure-storage-file-datalake==12.1.2
from azure.storage.filedatalake import DataLakeServiceClient 
from azure.storage.filedatalake import FileSystemClient
from azure.storage.filedatalake import DataLakeFileClient
pd.set_option("display.max_rows",     99999)
pd.set_option("display.max_columns",  99999)
pd.set_option("display.max_colwidth", 99999)

# COMMAND ----------

# MAGIC %md # Importing Function

# COMMAND ----------

def time_mask(start_date=None, end_date=None):
	# time mask
	if start_date is None and end_date is None:
		#today     = datetime.today().strftime('%Y-%m-%d')
		today      = date.today()
		#today      = pd.to_datetime('2020-10-01', format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		# today      = pd.to_datetime('2020-03-01', format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		#today      = pd.to_datetime('2020-04-01', format = '%Y-%m-%d', errors='coerce', yearfirst=True)

		# today is the start of the month
		if pd.to_datetime(today, format = '%Y-%m-%d', errors='coerce', yearfirst=True).day == 1:
			end_date   = datetime.combine((today - timedelta(days=1)), time.max) # Last Month's last day
			p = pd.Period(pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True).strftime('%Y-%m-%d')).days_in_month # Calculating the total days in the month
			start_date = pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True).date() - timedelta(days=p-1) # Don't doubt it! It truely is p-1. You can try and you will know that this is correct.
			end_date   = pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True)
			start_date = pd.to_datetime(start_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
			print(f'today:     {today}')
			print(f'Monitor Start Date: {start_date}')
			print(f'Monitor End   Date: {end_date}')

		else: # today is not the start of the month
			yesterday  = datetime.combine((today - timedelta(days=1)), time.max)
			start_date = (yesterday - timedelta(days=30-1)).date()
			today      = pd.to_datetime(today,      format = '%Y-%m-%d', errors='coerce', yearfirst=True)
			yesterday  = pd.to_datetime(yesterday,  format = '%Y-%m-%d', errors='coerce', yearfirst=True)
			start_date = pd.to_datetime(start_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
			#today = pd.to_datetime(today, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
			print(f'today:     {today}')
			print(f'Monitor Start Date: {start_date}')
			print(f'Monitor End   Date: {yesterday}')

	elif start_date is not None and end_date is  None:
		start_date = pd.to_datetime(start_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		end_date   = datetime.combine(start_date + timedelta(days=30-1), time.max)
		end_date   = pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		start_date = pd.to_datetime(start_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		print(f'Monitor Start Date: {start_date}')
		print(f'Monitor End   Date: {end_date}')

	elif start_date is None  and end_date is not None:
		end_date   = pd.to_datetime(end_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		end_date   = datetime.combine(end_date, time.max)
		start_date = pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True).date() - timedelta(days=30-1)
		end_date   = pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		start_date = pd.to_datetime(start_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		print(f'Monitor Start Date: {start_date}')
		print(f'Monitor End   Date: {end_date}')

	else: # start_date is not None  & end_date is not None
		end_date   = pd.to_datetime(end_date,   format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		end_date   = datetime.combine(end_date, time.max)
		start_date = pd.to_datetime(start_date, format = '%Y-%m-%d', errors='coerce', yearfirst=True)
		print(f'Monitor Start Date: {start_date}')
		print(f'Monitor End   Date: {end_date}')

	return start_date, end_date


# COMMAND ----------

def get_qlist(bu, analysis_theme):

	reject_theme_list  = ['reject',  'approve', '核准', '婉拒'] 
	default_theme_list = ['default', 'breach',  '違約'] 


	if bu.lower() == 'ccf':
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.03,        0.07,      0.15,     0.25,     0.25,      0.25]
			index         = ['極高度風險', '高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,      0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
	elif bu.lower() == 'ep':
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.25,      0.22,     0.22,      0.22,     0.09]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

	elif bu.lower() == 'ep_app':
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			print(f'{bu} {analysis_theme} NOT SUPPORTED!') 
			return None

	elif bu.lower() == 'gpc_ao' : 
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

	elif bu.lower() == 'gpc_oa_b' : 
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

	elif bu.lower() == 'gpc_oa_c' : 
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

	elif bu.lower() == 'gpc_oa_d' : 
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

	elif bu.lower() == 'gpc_oa_e' : 
		if analysis_theme.lower() in reject_theme_list:
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']
		else: # default_theme_list
			q_list        = [0.10,      0.15,     0.25,      0.25,     0.25]
			index         = ['高度風險', '中高風險', '中度風險', '中低風險', '低度風險']

	else:
		print(f'{bu} {analysis_theme} NOT SUPPORTED!') 
		return None

	return q_list, index


# COMMAND ----------

def monitoring_prediction(df_check, bu, analysis_theme, \
	start_date=None, end_date=None):
	
	reject_theme_list  = ['reject',  'approve', '核准', '婉拒'] 
	default_theme_list = ['default', 'breach', '違約'] 

	bu              = bu.lower()
	analysis_theme  = analysis_theme.lower()


	# Predefined parameter
	bu_column         = 'BU_TYPE'
	model_tag_column  = 'MODEL_TAG'
	stage_column      = 'STAGE'
	status_column     = 'STATUS'
	apply_date_column = 'APLY_DT'
	error_msg_column  = 'ERROR_MSG'
	risk_level_column = 'RISK_LEVEL'


	# Converting datetime 
	df_check[apply_date_column] = pd.to_datetime(df_check[apply_date_column], format = '%Y-%m-%d', errors='coerce', yearfirst=True)

	# time mask
	start_date, end_date = time_mask(start_date=start_date, end_date=end_date)

	time_mask1  = df_check[apply_date_column] <= end_date
	time_mask2  = df_check[apply_date_column] >= start_date

	df_check  =  df_check.loc[time_mask1 & time_mask2 ]
	df_check  =  df_check.reset_index(drop=True)

	# other masks
	# Check model tag
	if len(bu.split('_')) > 1: # GPC_AO, GPC_OA_B, GPC_OA_C, GPC_OA_D,GPC_OA_E   
		if   analysis_theme.lower() in reject_theme_list:
			mask1    = df_check[model_tag_column] == bu.upper()+'_Approve' 
		else: # analysis_theme.lower() in default_theme_list:
			mask1    = df_check[model_tag_column] == bu.upper()+'_Breach'
	else:
		mask1    = df_check[bu_column] == bu
	if   analysis_theme.lower() in reject_theme_list:
		mask2 =  df_check[stage_column] == '核准'
	elif analysis_theme.lower() in default_theme_list:
		mask2 =  df_check[stage_column] == '違約'
	else: 
		print(f'{bu} {analysis_theme} Drift_Report NOT SUPPORTED!') 
		#return None
	mask3 = df_check[status_column]     == 'Success'
	
	try:
		df_check       =  df_check.loc[mask1 & mask2 & mask3]
		df_check       =  df_check.reset_index(drop=True)
		total_case_num = len(df_check)
	except UnboundLocalError:
		print(f'{bu} {analysis_theme} Drift_Report NOT SUPPORTED!')
		return None


	# Reading index & q_list from file
	q_list, index = get_qlist(bu, analysis_theme)

	print(f'{bu} {analysis_theme} index : {index} \n \n ')
	print(f'{bu} {analysis_theme} q_list: {q_list} \n \n ')

	# df
	df = pd.DataFrame(data=np.zeros((len(index),4)), index=index, \
		columns=['Expected_Percentage', 'Predicted_Num','Predicted_Percentage', \
		'Diff'])

	df['Expected_Percentage'] = pd.Series(q_list,        index=index)


	# Total
	df_temp = pd.DataFrame(df.loc[index[-1],:]).T

	df = df.append(df_temp).reset_index(drop=True)
	del df_temp

	
	index.append('Total')
	df.index = index 
	
	df.loc['Total', 'Expected_Percentage']  = sum(df.loc[index[:-1],'Expected_Percentage'])
    
	# Calculating Predicted_Percentage
	index = df.index
	for i in index[:-1]:
		# print(i)
		try:
			df.loc[i, 'Predicted_Num'] = len(df_check.loc[df_check[risk_level_column]==i])
		except KeyError:
			df.loc[i, 'Predicted_Num'] = 0
	df.loc['Total',   'Predicted_Num']  = total_case_num


	# Calculating Predicted_Percentage
	if total_case_num == 0:
		df['Predicted_Percentage'] = 0
	else:
		df['Predicted_Percentage'] = df.loc[:, 'Predicted_Num'] / total_case_num

	# Calculating Diff
	df['Diff'] = df['Predicted_Percentage'] - df['Expected_Percentage']


	del df['Predicted_Num']


	df = df.fillna(0)

	return df

# COMMAND ----------

def monitoring_error_rate(df_check, bu, analysis_theme,dtl_dir,storage_account_name, storage_account_key,file_system_name, start_date=None, end_date=None):
	
	reject_theme_list  = ['reject',  'approve',  '核准', '婉拒'] 
	default_theme_list = ['default', 'breach',   '違約'] 
	gpc_lst            = ['gpc_ao',  'gpc_oa_b', 'gpc_oa_c', 'gpc_oa_d', 'gpc_oa_e']

	# Predefined parameter
	bu_column         = 'BU_TYPE'
	model_tag_column  = 'MODEL_TAG'
	stage_column      = 'STAGE'
	status_column     = 'STATUS'
	apply_date_column = 'APLY_DT'
	error_msg_column  = 'ERROR_MSG'
	risk_level_column = 'RISK_LEVEL'

	# Converting datetime 
	df_check[apply_date_column] = pd.to_datetime(df_check[apply_date_column], format = '%Y-%m-%d', errors='coerce', yearfirst=True)

	# time mask
	start_date, end_date = time_mask(start_date=start_date, end_date=end_date)

	time_mask1  = df_check[apply_date_column] <= end_date
	time_mask2  = df_check[apply_date_column] >= start_date

	df_check  =  df_check.loc[time_mask1 & time_mask2 ]
	df_check  =  df_check.reset_index(drop=True)


	# Other Masks
	# Check model tag
	if len(bu.split('_')) > 1: # GPC_AO, GPC_OA_B, GPC_OA_C, GPC_OA_D,GPC_OA_E   
		if   analysis_theme.lower() in reject_theme_list:
			mask1    = df_check[model_tag_column] == bu.upper()+'_Approve' 
		else: # analysis_theme.lower() in default_theme_list:
			print(f'{bu} {analysis_theme} Error_Rate_Report NOT SUPPORTED!') 
			return pd.DataFrame()
	else:
		mask1    = df_check[bu_column] == bu
	if   analysis_theme.lower() in reject_theme_list:
		mask2    =  df_check[stage_column] == '核准'
	else: 
		print(f'{bu} {analysis_theme} Error_Rate_Report NOT SUPPORTED!') 
		return pd.DataFrame()
	mask3    = df_check[status_column]    == 'Success'
	
	try:
		df_check       =  df_check.loc[mask1 & mask2 & mask3]
		df_check       =  df_check.reset_index(drop=True)
		total_case_num = len(df_check)
	except UnboundLocalError:
		print('NOT SUPPORTED!')
		return pd.DataFrame()


	try:  
		file_list            = []
		file_type            = '.csv'
		#storage_account_key  = "ygM6jtNW7+3zu5JjY+FWFbclf78OR3l9bZpeMeZKPy+73VX93q7OD3qeQqYBgWCkUu3cTIxlLergvXMBaj4HyQ=="

		service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
		"https", storage_account_name), credential=storage_account_key)

		# Get File System List
		filesystem_client = service_client.get_file_system_client(file_system=file_system_name)

	except Exception as e:
		print(e)

	
	if analysis_theme.lower() in reject_theme_list:
		
		file_list = [dtl_dir] # '10997/10997/ccf_acs_app_dtl.out'

		if type(dtl_dir) == list: # GPC
			gpc_dtl_dir   = dtl_dir[0]
			gpc_inves_dir = dtl_dir[1]

			print(f'{gpc_dtl_dir}')
			download_client  = filesystem_client.get_file_client(file_path=gpc_dtl_dir)
			getfile = download_client.download_file()
			print('Reading df Done!')
			df    = pd.read_csv( StringIO(str(getfile.readall(),'utf-8 sig')), \
			                  encoding='utf-8 sig', sep='\|\|', index_col=None, error_bad_lines=False, engine='python', usecols=lambda x: x in ['APP_NO', 'MAJ_SEQ_ID', 'APR_CASE_NO', 'CRD_USR_NME', 'APP_CRD_STATUS_CD', 'APR_INVES_RESULT', 'APLY_DT', '#APP_NO', '#MAJ_SEQ_ID', '#APR_CASE_NO','#CRD_USR_NME', '#APP_CRD_STATUS_CD', '#APR_INVES_RESULT','#APLY_DT'])
			df    = df.rename(columns=lambda x: x.strip('#'))
			print('Renaming df Done!')
			#print(f'{gpc_dtl_dir} Done!', '\n')

			print(f'{gpc_inves_dir}')
			download_client  = filesystem_client.get_file_client(file_path=gpc_inves_dir)
			getfile = download_client.download_file()
			print('Reading df Done!')
			df_inves    = pd.read_csv( StringIO(str(getfile.readall(),'utf-8 sig')), \
			                  encoding='utf-8 sig', sep='\|\|', index_col=None, error_bad_lines=False, engine='python', usecols=lambda x: x in ['APP_NO', 'MAJ_SEQ_ID', 'APR_CASE_NO', 'CRD_USR_NME', 'APP_CRD_STATUS_CD', 'APR_INVES_RESULT', 'APLY_DT', '#APP_NO', '#MAJ_SEQ_ID', '#APR_CASE_NO','#CRD_USR_NME', '#APP_CRD_STATUS_CD', '#APR_INVES_RESULT','#APLY_DT'])
			df_inves    = df_inves.rename(columns=lambda x: x.strip('#'))
			print('Renaming df Done!')
			#print(f'{gpc_inves_dir} Done!', '\n')

			df_dtl = pd.merge(df, df_inves, how='inner', on=['MAJ_SEQ_ID', 'APR_CASE_NO'])
			df_dtl = df_dtl.dropna().reset_index(drop=True)
			del df, df_inves

		else:
			for file in file_list:
				print(f'{file}')
				download_client  = filesystem_client.get_file_client(file_path=file)
				getfile = download_client.download_file()
				print('Reading df Done!')
				df_dtl    = pd.read_csv( StringIO(str(getfile.readall(),'utf-8 sig')), \
				                  encoding='utf-8 sig', sep='\|\|', index_col=None, error_bad_lines=False, engine='python', usecols=lambda x: x in ['APP_NO', 'MAJ_SEQ_ID', 'APR_CASE_NO', 'CRD_USR_NME', 'APP_CRD_STATUS_CD', 'APR_INVES_RESULT', 'APLY_DT', '#APP_NO', '#MAJ_SEQ_ID', '#APR_CASE_NO','#CRD_USR_NME', '#APP_CRD_STATUS_CD', '#APR_INVES_RESULT','#APLY_DT'])
				df_dtl    = df_dtl.rename(columns=lambda x: x.strip('#'))
				print('Renaming df Done!')
				#print(f'{file_name} Done!', '\n')
		
		if bu.lower() == 'ccf':
			# df_dtl = pd.read_csv('/dbfs/user/monitor_test/ccf_acs_app_dtl.csv')

			# Ultimate y: APP_CRD_STATUS_CD
			df_dtl = df_dtl.rename(columns={'APP_CRD_STATUS_CD': 'Not_Approved'})
			mask1 = (df_dtl['Not_Approved'] == 'Y')
			mask2 = (df_dtl['Not_Approved'] == 'N')
			df_dtl = df_dtl[mask1|mask2].reset_index(drop=True)
	
			# Filtering Out CRD_USR_NME == 虛擬審員
			df_dtl = df_dtl.loc[df_dtl['CRD_USR_NME']!='虛擬審員'].reset_index(drop=True)

		elif bu.lower() == 'ep':

			# df_dtl = pd.read_csv('/dbfs/user/monitor_test/ccf_acs_ep_dtl.csv')	

			# Ultimate y: APP_CRD_STATUS_CD
			df_dtl = df_dtl.rename(columns={'APP_CRD_STATUS_CD': 'Not_Approved'})
			mask1  = (df_dtl['Not_Approved'] == 'Y')
			mask2  = (df_dtl['Not_Approved'] == 'N')
			df_dtl = df_dtl[mask1|mask2].reset_index(drop=True)
			

		elif bu.lower() in gpc_lst:
			
			# df_dtl = pd.read_csv('/dbfs/user/monitor_test/gpc_acs_app_dtl.csv')	

			# Ultimate y: APR_INVES_RESULT
			df_dtl = df_dtl.rename(columns={'APR_INVES_RESULT': 'Not_Approved'})
			mask1 = (df_dtl['Not_Approved'] == 'A')
			mask2 = (df_dtl['Not_Approved'] == 'B')
			mask3 = (df_dtl['Not_Approved'] == 'C')
			mask4 = (df_dtl['Not_Approved'] == 'D')
			df_dtl = df_dtl[mask1|mask2|mask3|mask4].reset_index(drop=True)
			df_dtl['Not_Approved'] = df_dtl['Not_Approved'].replace({'A':'N', 'B':'Y', 'C':'Y', 'D':'Y'})
			# df_dtl['Not_Approved'] = df_dtl['Not_Approved'].replace({'A':0, 'B':1, 'C':1, 'D':1})
		else:
			print(f'BU NOT SUPPORTED!') 
			print(f'{bu} {analysis_theme} NOT SUPPORTED!') 
	else:
		print(f'analysis_theme NOT SUPPORTED!') 
		print(f'{bu} {analysis_theme} NOT SUPPORTED!') 

	# Renaming "MAJ_SEQ_ID" to  "CASE_ID"
	df_dtl = df_dtl.rename(columns={"MAJ_SEQ_ID": "CASE_ID"})	

	# Joining
	df_merge = pd.merge(df_dtl, df_check, how='inner', on=['CASE_ID'])
	# df_merge = pd.merge(df_dtl, df_check, how='inner', on=['CASE_ID', 'APLY_DT'])
	del df_dtl


	# Reading min_prob_list, index & q_list from file
	q_list, index = get_qlist(bu, analysis_theme)

	print(f'{bu} {analysis_theme} index : {index} \n \n ')
	print(f'{bu} {analysis_theme} q_list: {q_list} \n \n ')


	# df
	df = pd.DataFrame(data=np.zeros((len(index),9)), index=index, \
		columns=['Approved', 'NotApproved', 'Predicted_Num', 'Approved_Percentage', 'NotApproved_Percentage', \
		'ErrorRate', 'Predicted_Percentage', 'Expected_Percentage', 'Diff'])

	
	df['Expected_Percentage'] = pd.Series(q_list,        index=index)


    
	# Approve  & NotApproved  & Predicted_Num: 
	# Approve% & NotApproved_Percentage :
	for i in index:
		try:
			df.loc[i, 'Approved']      = len(df_merge.loc[(df_merge[risk_level_column]==i) & \
		(df_merge['Not_Approved']=='N')]) 
		except Exception as e:
			df.loc[i, 'Approved']      = 0

		try:
			df.loc[i, 'NotApproved']   = len(df_merge.loc[(df_merge[risk_level_column]==i) & \
	(df_merge['Not_Approved']=='Y')]) 
		except Exception as e:
			df.loc[i, 'NotApproved']      = 0

		df.loc[i, 'Predicted_Num'] = df.loc[i, 'Approved']    + df.loc[i, 'NotApproved']

		try:
			df.loc[i, 'Approved_Percentage']     = df.loc[i, 'Approved']    / df.loc[i, 'Predicted_Num']
		except Exception as e:
			df.loc[i, 'Approved_Percentage']     = 0

		try:
			df.loc[i, 'NotApproved_Percentage']     = df.loc[i, 'NotApproved']    / df.loc[i, 'Predicted_Num']
		except Exception as e:
			df.loc[i, 'NotApproved_Percentage']     = 0
	
	df.loc[index,['Approved', 'NotApproved', 'Predicted_Num', 'Approved_Percentage', 'NotApproved_Percentage']] = df.loc[index,['Approved', 'NotApproved', 'Predicted_Num', 'Approved_Percentage', 'NotApproved_Percentage']].fillna(0)

	# Total
	df.loc['Total', 'Approved']              = sum(df.loc[index, 'Approved'])
	df.loc['Total', 'NotApproved']           = sum(df.loc[index, 'NotApproved'])
	df.loc['Total', 'Predicted_Num']         = sum(df.loc[index, 'Predicted_Num'])
	
	df.loc['Total', 'Approved_Percentage']             = sum(df.loc[index, 'Approved_Percentage'])
	df.loc['Total', 'NotApproved_Percentage']          = sum(df.loc[index, 'NotApproved_Percentage'])
	df.loc['Total', 'Predicted_Percentage'] = sum(df.loc[index, 'Predicted_Percentage'])

	# Calculating Predicted_Percentage
	if total_case_num != 0:
		df['Predicted_Percentage'] = df.loc[:, 'Predicted_Num'] / total_case_num
	else: 
		df['Predicted_Percentage'] = 0


	# Calculating Total Expected_Percentage
	df.loc['Total', 'Expected_Percentage']  = 1

	# Calculating Diff
	df['Diff'] = df['Predicted_Percentage'] - df['Expected_Percentage']

	# Calculating ErrorRate%
	if bu.lower() == 'ccf':
		df.loc['極高度風險', 'ErrorRate'] = len(df_merge.loc[(df_merge[risk_level_column]=='極高度風險') & \
		(df_merge['Not_Approved']=='N')]) / df.loc['極高度風險', 'Predicted_Num']
		df.loc['高度風險',   'ErrorRate'] = len(df_merge.loc[(df_merge[risk_level_column]=='高度風險') & \
		(df_merge['Not_Approved']=='N')]) / df.loc['高度風險', 'Predicted_Num']
		df.loc['中高風險',   'ErrorRate'] = None
		df.loc['中度風險',   'ErrorRate'] = None
		df.loc['中低風險',   'ErrorRate'] = None
		df.loc['低度風險',   'ErrorRate'] = len(df_merge.loc[(df_merge[risk_level_column]=='低度風險') & \
		(df_merge['Not_Approved']=='Y')]) / df.loc['低度風險', 'Predicted_Num']
		df.loc['Total',     'ErrorRate'] = (len(df_merge.loc[(df_merge[risk_level_column]=='極高度風險') & \
		(df_merge['Not_Approved']=='N')]) + len(df_merge.loc[(df_merge[risk_level_column]=='低度風險') & \
		(df_merge['Not_Approved']=='Y')]))/ (df.loc['極高度風險', 'Predicted_Num'] + df.loc['低度風險', 'Predicted_Num'])
	else:
		df.loc['高度風險',   'ErrorRate'] = len(df_merge.loc[(df_merge[risk_level_column]=='高度風險') & \
		(df_merge['Not_Approved']=='N')]) / df.loc['高度風險', 'Predicted_Num']
		df.loc['中高風險',   'ErrorRate'] = None
		df.loc['中度風險',   'ErrorRate'] = None
		df.loc['中低風險',   'ErrorRate'] = None
		df.loc['低度風險',   'ErrorRate'] = len(df_merge.loc[(df_merge[risk_level_column]=='低度風險') & \
		(df_merge['Not_Approved']=='Y')]) / df.loc['低度風險', 'Predicted_Num']
		df.loc['Total',     'ErrorRate'] = (len(df_merge.loc[(df_merge[risk_level_column]=='高度風險') & \
		(df_merge['Not_Approved']=='N')]) + len(df_merge.loc[(df_merge[risk_level_column]=='低度風險') & \
		(df_merge['Not_Approved']=='Y')]))/ (df.loc['高度風險', 'Predicted_Num'] + df.loc['低度風險', 'Predicted_Num'])

	df = df.fillna(0)

	return df

# COMMAND ----------

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

looping_list = [('approve',  'CCF'),    ('approve',  'EP'), \
				('approve',  'GPC_AO'), ('approve',  'GPC_OA_D'), \
				('default',  'CCF'),    ('default',  'EP'), \
				('default',  'GPC_AO'), ('default',  'GPC_OA_D')]

# 保留一週時間確保案件已走完流程
check_day  = (datetime.today()-timedelta(days=7)).strftime('%Y-%m-%d')
month_day  = (datetime.today()-timedelta(days=37)).strftime('%Y-%m-%d')
quart_day  = (datetime.today()-timedelta(days=127)).strftime('%Y-%m-%d')
output_day  = datetime.today().strftime('%Y%m%d')

blob_output_dir_1 = f'/ITRI/Report/Monitor/{output_day}/total/'
blob_output_dir_2 = f'/ITRI/Report/Monitor/{output_day}/month/'
blob_output_dir_3 = f'/ITRI/Report/Monitor/{output_day}/quart/'

for loop in looping_list:
  analysis_theme = loop[0]
  bu             = loop[1]
  
  if bu.lower() == 'ccf':
    dtl_dir = storage_file_path+'ccf_acs_app_dtl.out'
  elif bu.lower() == 'ep':
    dtl_dir = storage_file_path+'ccf_acs_ep_dtl.out'
  else:
    dtl_dir = [storage_file_path+'gpc_acs_app_dtl.out', storage_file_path+'gpc_acs_inves.out']
  
  print(f'{bu} {analysis_theme}')
    
  func = Monitor(mysql_host=mysql_host, mysql_user=mysql_user, \
                   mysql_password=mysql_password, mysql_ssl_ca=mysql_ssl_ca, \
                   bu=bu, analysis_theme=analysis_theme, \
                   storage_account_name=storage_account_name, \
                   storage_account_key=storage_account_key, \
                   file_system_name=storage_file_system, \
                   dtl_dir    = dtl_dir, \
                   blob_output_dir = blob_output_dir_1, \
                   start_date='2020-08-01', end_date=check_day)
  df_prediction, df_error_rate = func.Reproting()
    
  func = Monitor(mysql_host=mysql_host, mysql_user=mysql_user, \
                   mysql_password=mysql_password, mysql_ssl_ca=mysql_ssl_ca, \
                   bu=bu, analysis_theme=analysis_theme, \
                   storage_account_name=storage_account_name, \
                   storage_account_key=storage_account_key, \
                   file_system_name=storage_file_system, \
                   dtl_dir    = dtl_dir, \
                   blob_output_dir = blob_output_dir_2, \
                   start_date=month_day, end_date=check_day)
  
  func = Monitor(mysql_host=mysql_host, mysql_user=mysql_user, \
                   mysql_password=mysql_password, mysql_ssl_ca=mysql_ssl_ca, \
                   bu=bu, analysis_theme=analysis_theme, \
                   storage_account_name=storage_account_name, \
                   storage_account_key=storage_account_key, \
                   file_system_name=storage_file_system, \
                   dtl_dir    = dtl_dir, \
                   blob_output_dir = blob_output_dir_3, \
                   start_date=quart_day, end_date=check_day)
  
  df_prediction, df_error_rate = func.Reproting()

# COMMAND ----------


