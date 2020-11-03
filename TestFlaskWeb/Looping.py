

# COMMAND ----------
class Looping:

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
