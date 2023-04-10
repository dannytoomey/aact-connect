import os, sys, psycopg2, argparse
import pandas as pd
import pandas.io.sql as sqlio
from functools import reduce
import progressbar, time

cwd = os.getcwd()

if os.path.exists(cwd+"/private/myconfig.txt"):
  with open(cwd+"/private/myconfig.txt") as file:
    credentials = file.readlines()

  parser = argparse.ArgumentParser()
  parser.add_argument("-s", "--search", action="store_true", required=False)
  parser.add_argument("-a", "--add", action="store_true", required=False)
  parser.add_argument("-l", "--lookup", required=False)
  parser.add_argument("-ae", "--AE_num", required=False)
  
  args = parser.parse_args()

  aact_connect = psycopg2.connect(
    host = "aact-db.ctti-clinicaltrials.org",
    user = str(credentials[0]).replace('\n',''),
    password = str(credentials[1]),
    database = "aact",
    port = 5432
  )

else:
  parser = argparse.ArgumentParser()
  parser.add_argument("-u", "--username", required=True)
  parser.add_argument("-p", "--password", required=True)
  parser.add_argument("-s", "--search", action="store_true", required=False)
  parser.add_argument("-a", "--add", action="store_true", required=False)
  parser.add_argument("-l", "--lookup", required=False)
  parser.add_argument("-ae", "--AE_num", required=False)
  
  args = parser.parse_args()

  aact_connect = psycopg2.connect(
  	host = "aact-db.ctti-clinicaltrials.org",
  	user = str(args.username),
  	password = str(args.password),
  	database = "aact",
  	port = 5432
  )


if args.search:

  print("Performing query...")

  cursor = aact_connect.cursor()

  
  query = '''

  select * from studies where    
  study_type = 'Interventional'   
  and   
  enrollment < 1000   
  and   
  (   
  (official_title ilike '%challenge%') or   
  (official_title ilike '%immunization%' and official_title ilike '%sporozoites%') or   
  (official_title ilike '%human%' and official_title ilike '%carriage%') or   
  (official_title ilike '%infection%' and   
  (official_title ilike '%controlled%' or official_title ilike '%experimental%' or official_title ilike '%induced%')) or    
  (official_title ilike '%efficacy%' and official_title ilike '%vaccine%') or   
  (official_title ilike '%human%' and official_title ilike '%exposure%') or   
  (official_title ilike '%healthy%' and   
  (official_title ilike '%naïve%' or official_title ilike '%naive%')) or    
  (official_title ilike '%competitive%' and official_title ilike '%carriage%')    
  OR    
  (brief_title ilike '%challenge%') or    
  (brief_title ilike '%immunization%' and brief_title ilike '%sporozoites%') or   
  (brief_title ilike '%human%' and brief_title ilike '%carriage%') or   
  (brief_title ilike '%infection%' and    
  (brief_title ilike '%controlled%' or brief_title ilike '%experimental%' or brief_title ilike '%induced%')) or   
  (brief_title ilike '%efficacy%' and brief_title ilike '%vaccine%') or   
  (brief_title ilike '%human%' and brief_title ilike '%exposure%') or   
  (brief_title ilike '%healthy%' and    
  (brief_title ilike '%naïve%' or brief_title ilike '%naive%')) or    
  (brief_title ilike '%competitive%' and brief_title ilike '%carriage%')    
  OR    
  (acronym ilike '%challenge%') or    
  (acronym ilike '%human%')   
  OR    
  nct_id IN   
  (select s.nct_id from studies s, keywords k where   
  s.nct_id = k.nct_id and k.name ilike '%challenge%')   
  OR    
  nct_id IN   
  (select s.nct_id from studies s, detailed_descriptions d where    
  s.nct_id = d.nct_id and   
  ((d.description ilike '%challenge%') and    
  (d.description ilike '%infection%' or   
  d.description ilike '%controlled%' or   
  d.description ilike '%experimental%')))   
  OR    
  nct_id IN   
  (select s.nct_id from studies s, brief_summaries b where    
  s.nct_id = b.nct_id and   
  ((b.description ilike '%challenge%') and    
  (b.description ilike '%infection%' or   
  b.description ilike '%controlled%' or   
  b.description ilike '%experimental%')))   
  )   

  
  '''
  


  data = sqlio.read_sql_query(query,aact_connect)
  query_frame = pd.DataFrame(data)
  query_frame.to_csv(cwd+"/queries/query_results.csv",header=True,index=True)

  read_csv = pd.read_csv(cwd+"/queries/query_results.csv")
  count = len(read_csv) 
  print("Query returned "+str(count)+" results")

  if os.path.exists(cwd+"/queries/query_"+str(count)+"_results.csv"):
    os.remove(cwd+"/queries/query_"+str(count)+"_results.csv")
    os.rename(cwd+"/queries/query_results.csv",cwd+"/queries/query_"+str(count)+"_results.csv")
  else:
    os.rename(cwd+"/queries/query_results.csv",cwd+"/queries/query_"+str(count)+"_results.csv")


if args.add:

  print("Adding additional results...")

  query_frame = pd.read_csv("AACT_6_5085_results.csv")

  nct_id_array = query_frame['nct_id'].to_list() # ['NCT01895855','NCT04150250'] 
  
  widgets = [' [',progressbar.Timer(format= 'elapsed time: %(elapsed)s'),'] ',progressbar.Bar('*'),' (',progressbar.ETA(), ') ',]
  bar = progressbar.ProgressBar(max_value=200,widgets=widgets).start()

  for nct_id in nct_id_array:
    
    trial = pd.DataFrame(query_frame.loc[query_frame['nct_id'] == nct_id])

    query_add_1 = "select number_of_nsae_subjects from calculated_values where nct_id = '"+nct_id+"'"
    query_add_2 = "select minimum_age_num from calculated_values where nct_id = '"+nct_id+"'"
    query_add_3 = "select maximum_age_num from calculated_values where nct_id = '"+nct_id+"'"
    query_add_4 = "select recruitment_details from participant_flows where nct_id = '"+nct_id+"'"
    query_add_5 = "select ci_percent, p_value from outcome_analyses where nct_id = '"+nct_id+"'"
    query_add_6 = "select description from design_groups where nct_id = '"+nct_id+"'"
    query_add_7 = "select description from interventions where nct_id = '"+nct_id+"'"
    query_add_8 = "select pmid, citation from study_references where nct_id = '"+nct_id+"'"
    query_add_9 = "select SUM(subjects_affected) AS AE_Count from reported_event_totals where nct_id = '"+nct_id+"' AND classification = 'Total, other adverse events'"
    query_add_10 = "select SUM(subjects_affected) AS SAE_Count from reported_event_totals where nct_id = '"+nct_id+"' AND classification = 'Total, serious adverse events'"
    query_add_11 = "select SUM(subjects_affected) AS Mortality_Count from reported_event_totals where nct_id = '"+nct_id+"' AND classification = 'Total, all-cause mortality'"
    query_add_12 = "select COUNT(DISTINCT adverse_event_term) AS Num_AEs_described from reported_events where nct_id = '"+nct_id+"'"
  
    
    data_add_1 = sqlio.read_sql_query(query_add_1,aact_connect)
    query_frame_add_1 = pd.DataFrame(data_add_1)
    query_frame_add_1['nct_id'] = nct_id
    query_frame_add_1 = query_frame_add_1.iloc[:1]
    data_concat = trial.merge(query_frame_add_1,on="nct_id",how="left")
    
    data_add_2 = sqlio.read_sql_query(query_add_2,aact_connect)
    query_frame_add_2 = pd.DataFrame(data_add_2)
    query_frame_add_2['nct_id'] = nct_id
    query_frame_add_2 = query_frame_add_2.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_2,on="nct_id",how="left")
    
    data_add_3 = sqlio.read_sql_query(query_add_3,aact_connect)
    query_frame_add_3 = pd.DataFrame(data_add_3)
    query_frame_add_3['nct_id'] = nct_id
    query_frame_add_3 = query_frame_add_3.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_3,on="nct_id",how="left")
    
    query_add_4 = "select recruitment_details from participant_flows where nct_id = '"+nct_id+"'"
    data_add_4 = sqlio.read_sql_query(query_add_4,aact_connect)
    query_frame_add_4 = pd.DataFrame(data_add_4)
    query_frame_add_4['nct_id'] = nct_id
    query_frame_add_4 = query_frame_add_4.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_4,on="nct_id",how="left")
   
    query_add_5 = "select ci_percent, p_value from outcome_analyses where nct_id = '"+nct_id+"'"
    data_add_5 = sqlio.read_sql_query(query_add_5,aact_connect)
    query_frame_add_5 = pd.DataFrame(data_add_5)
    query_frame_add_5 = query_frame_add_5.iloc[:1]
    query_frame_add_5['nct_id'] = nct_id
    data_concat = data_concat.merge(query_frame_add_5,on="nct_id",how="left")

    data_add_6 = sqlio.read_sql_query(query_add_6,aact_connect)
    query_frame_add_6 = pd.DataFrame(data_add_6)
    query_frame_add_6['nct_id'] = nct_id
    query_frame_add_6 = query_frame_add_6.iloc[:1]
    query_frame_add_6 = query_frame_add_6.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_6,on="nct_id",how="left")

    data_add_7 = sqlio.read_sql_query(query_add_7,aact_connect)
    query_frame_add_7 = pd.DataFrame(data_add_7)
    query_frame_add_7['nct_id'] = nct_id
    query_frame_add_7 = query_frame_add_7.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_7,on="nct_id",how="left")

    data_add_8 = sqlio.read_sql_query(query_add_8,aact_connect)
    query_frame_add_8 = pd.DataFrame(data_add_8)
    query_frame_add_8['nct_id'] = nct_id
    query_frame_add_8 = query_frame_add_8.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_8,on="nct_id",how="left")

    data_add_9 = sqlio.read_sql_query(query_add_9,aact_connect)
    query_frame_add_9 = pd.DataFrame(data_add_9)
    query_frame_add_9['nct_id'] = nct_id
    query_frame_add_9 = query_frame_add_9.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_9,on="nct_id",how="left")

    data_add_10 = sqlio.read_sql_query(query_add_10,aact_connect)
    query_frame_add_10 = pd.DataFrame(data_add_10)
    query_frame_add_10['nct_id'] = nct_id
    query_frame_add_10 = query_frame_add_10.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_10,on="nct_id",how="left")

    data_add_11 = sqlio.read_sql_query(query_add_11,aact_connect)
    query_frame_add_11 = pd.DataFrame(data_add_11)
    query_frame_add_11['nct_id'] = nct_id
    query_frame_add_11 = query_frame_add_11.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_11,on="nct_id",how="left")

    data_add_12 = sqlio.read_sql_query(query_add_12,aact_connect)
    query_frame_add_12 = pd.DataFrame(data_add_12)
    query_frame_add_12['nct_id'] = nct_id
    query_frame_add_12 = query_frame_add_12.iloc[:1]
    data_concat = data_concat.merge(query_frame_add_12,on="nct_id",how="left")
    

    if nct_id == nct_id_array[0]:
      result_concat = data_concat
      
    else:
      result_concat = data_concat.append(result_concat,ignore_index=True)
      
    bar.update((nct_id_array.index(nct_id)/len(nct_id_array))*200)

  result_concat.to_csv(cwd+"/additional_data/query_additional_data.csv",header=True,index=False)

  read_csv = pd.read_csv(cwd+"/additional_data/query_additional_data.csv")
  count = len(read_csv) 
  print("Added additional data for "+str(count)+" results")

  if os.path.exists(cwd+"/additional_data/query_"+str(count)+"_additional_data.csv"):
    os.remove(cwd+"/additional_data/query_"+str(count)+"_additional_data.csv")
    os.rename(cwd+"/additional_data/query_additional_data.csv",cwd+"/additional_data/query_"+str(count)+"_additional_data.csv")
  else:
    os.rename(cwd+"/additional_data/query_additional_data.csv",cwd+"/additional_data/query_"+str(count)+"_additional_data.csv")

if args.lookup != None:

  print("Looking up AE data for "+args.lookup+"...")

  query_lookup_1 = "select title, param_value, param_type from outcome_measurements where nct_id = '"+args.lookup+"' and (title ilike '%symptom%' or title ilike '%adverse%')"
  query_lookup_2 = "select classification, subjects_affected, subjects_at_risk from reported_event_totals where nct_id = '"+args.lookup+"'"
  query_lookup_3 = "select subjects_affected, subjects_at_risk, event_count, adverse_event_term from reported_events where nct_id = '"+args.lookup+"'"

  queries = [query_lookup_1,query_lookup_2,query_lookup_3]

  for query_add in queries:
    data_add = sqlio.read_sql_query(query_add,aact_connect)
    query_frame = pd.DataFrame(data_add)
    query_frame['nct_id'] = str(args.lookup)
    if query_add == queries[0]:
      data_concat = query_frame
    else:
      data_concat = data_concat.append(query_frame,ignore_index=True)
  
  data_concat.to_csv(cwd+"/AE_lookup/AE_lookup_"+args.lookup+".csv",header=True,index=False)

if args.AE_num != None:

  print("Couting numbers for AEs for "+args.AE_num+"...")
  
  query_lookup_1 = "select COUNT(DISTINCT adverse_event_term) AS Num_AEs_described from reported_events where nct_id = '"+args.AE_num+"'"
  
  data_add = sqlio.read_sql_query(query_lookup_1,aact_connect)
  query_frame = pd.DataFrame(data_add)
  query_frame['nct_id'] = str(args.AE_num)
  data_concat = query_frame

  data_concat.to_csv(cwd+"/AE_lookup/AE_count_"+args.AE_num+".csv",header=True,index=False) 


