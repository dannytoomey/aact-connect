import os, sys, psycopg2, argparse
import pandas as pd
import pandas.io.sql as sqlio
from functools import reduce
import progressbar, time
from datetime import datetime

cwd = os.getcwd()

parser = argparse.ArgumentParser()

if os.path.exists(cwd+"/private/myconfig.txt"):
  with open(cwd+"/private/myconfig.txt") as file:
    credentials = file.readlines()
  user = str(credentials[0]).replace('\n','')
  password = str(credentials[1])

else:
  parser.add_argument("-u", "--username", required=True)
  parser.add_argument("-p", "--password", required=True)
  user = str(args.username)
  password = str(args.password)

parser.add_argument("-s", "--search", required=False)
parser.add_argument("-a", "--add", action="store_true", required=False)
parser.add_argument("-us", "--use_search", required=False)
parser.add_argument("-l", "--lookup", required=False)
parser.add_argument("-ae", "--AE_num", required=False)

args = parser.parse_args()

aact_connect = psycopg2.connect(
  host = "aact-db.ctti-clinicaltrials.org",
  user = user,
  password = password,
  database = "aact",
  port = 5432
)


if args.search:

  print("Performing query...")

  cursor = aact_connect.cursor()

  if os.path.exists(cwd+'/'+args.search):
    with open(cwd+'/'+args.search) as file:
      read_file = file.read()
    query = str(read_file).replace('\n',' ')
  else:
    aact_connect.close()
    sys.exit('Error: Please add your search query to the `query_text` directory as a .txt file')

  data = sqlio.read_sql_query(query,aact_connect)
  query_frame = pd.DataFrame(data)
  query_frame.to_csv(cwd+"/query_results/query_results.csv",header=True,index=True)

  read_csv = pd.read_csv(cwd+"/query_results/query_results.csv")
  count = len(read_csv)
  print("Query returned "+str(count)+" results")

  if os.path.exists(cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv"):
    os.remove(cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv")
    os.rename(cwd+"/query_results/query_results.csv",cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv")
  else:
    os.rename(cwd+"/query_results/query_results.csv",cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv")

  last_search = cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv"

  if(args.add == None and args.lookup == None and args.AE_num == None):
    aact_connect.close()

  
if args.add:

  print("Adding additional results...")

  if(args.use_search != None):
    query_frame = pd.read_csv(cwd+"/"+args.use_search)
  else:
    query_frame = pd.read_csv(last_search)

  nct_id_array = query_frame['nct_id'].to_list() 
  
  widgets = [' [',progressbar.Timer(format= 'elapsed time: %(elapsed)s'),'] ',progressbar.Bar('*'),' (',progressbar.ETA(), ') ',]
  bar = progressbar.ProgressBar(max_value=200,widgets=widgets).start()

  for nct_id in nct_id_array:
    
    trial = pd.DataFrame(query_frame.loc[query_frame['nct_id'] == nct_id])

    combined_query_1 = '''\
    select calculated_values.nct_id, calculated_values.number_of_nsae_subjects, calculated_values.minimum_age_num, calculated_values.maximum_age_num,
           participant_flows.recruitment_details,
           outcome_analyses.ci_percent,outcome_analyses.p_value,
           design_groups.description,
           interventions.description,
           study_references.pmid,study_references.citation,
           COUNT(DISTINCT reported_events.adverse_event_term) AS Num_AEs_described 

    from calculated_values
    left join participant_flows
      on participant_flows.nct_id = calculated_values.nct_id 
    left join outcome_analyses
      on outcome_analyses.nct_id = calculated_values.nct_id 
    left join design_groups
      on design_groups.nct_id = calculated_values.nct_id 
    left join interventions
      on interventions.nct_id = calculated_values.nct_id 
    left join study_references
      on study_references.nct_id = calculated_values.nct_id
    left join reported_events
      on reported_events.nct_id = calculated_values.nct_id

    where 
      calculated_values.nct_id = '{}' 

    group by
      calculated_values.nct_id, calculated_values.number_of_nsae_subjects, calculated_values.minimum_age_num, calculated_values.maximum_age_num,
      participant_flows.recruitment_details,
      outcome_analyses.ci_percent,outcome_analyses.p_value,
      design_groups.description,
      interventions.description,
      study_references.pmid,study_references.citation
    
    fetch first 1 rows only
          
    \
    '''.format(nct_id)

    combined_query_2 = '''\
    select reported_event_totals.nct_id,
           sum(case when reported_event_totals.classification = 'Total, other adverse events' then 
              reported_event_totals.subjects_affected else 0 end) as AE_Count,
           sum(case when reported_event_totals.classification = 'Total, serious adverse events' then 
              reported_event_totals.subjects_affected else 0 end) as SAE_Count,
           sum(case when reported_event_totals.classification = 'Total, all-cause mortality' then 
              reported_event_totals.subjects_affected else 0 end) as Mortality_Count
    from reported_event_totals

    where 
      reported_event_totals.nct_id = '{}' 
    
    group by
      reported_event_totals.nct_id

    \
    '''.format(nct_id)


    data_add_1 = sqlio.read_sql_query(combined_query_1,aact_connect)
    data_concat = trial.merge(data_add_1,on="nct_id",how="left")
    data_add_2 = sqlio.read_sql_query(combined_query_2,aact_connect)
    data_concat = data_concat.merge(data_add_2,on="nct_id",how="left")

    if nct_id == nct_id_array[0]:
      result_concat = data_concat
    else:
      result_concat = data_concat.append(result_concat,ignore_index=True)
      
    bar.update((nct_id_array.index(nct_id)/len(nct_id_array))*200)

  result_concat.to_csv(cwd+"/additional_data/query_additional_data.csv",header=True,index=False)

  read_csv = pd.read_csv(cwd+"/additional_data/query_additional_data.csv")
  count = len(read_csv)
  print("\nAdded additional data for "+str(count)+" results")

  if os.path.exists(cwd+"/additional_data/query_"+str(count)+"_additional_data_"+datetime.today().strftime('%Y-%m-%d')+".csv"):
    os.remove(cwd+"/additional_data/query_"+str(count)+"_additional_data_"+datetime.today().strftime('%Y-%m-%d')+".csv")
    os.rename(cwd+"/additional_data/query_additional_data.csv",cwd+"/additional_data/query_"+str(count)+"_additional_data_"+datetime.today().strftime('%Y-%m-%d')+".csv")
  else:
    os.rename(cwd+"/additional_data/query_additional_data.csv",cwd+"/additional_data/query_"+str(count)+"_additional_data_"+datetime.today().strftime('%Y-%m-%d')+".csv")
  
  aact_connect.close()

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
  
  data_concat.to_csv(cwd+"/AE_lookup/AE_lookup_"+args.lookup+"_"+datetime.today().strftime('%Y-%m-%d')+".csv",header=True,index=False)    

  aact_connect.close()

if args.AE_num != None:

  print("Couting numbers for AEs for "+args.AE_num+"...")
  
  query_lookup_1 = "select COUNT(DISTINCT adverse_event_term) AS Num_AEs_described from reported_events where nct_id = '"+args.AE_num+"'"
  
  data_add = sqlio.read_sql_query(query_lookup_1,aact_connect)
  query_frame = pd.DataFrame(data_add)
  query_frame['nct_id'] = str(args.AE_num)
  data_concat = query_frame

  data_concat.to_csv(cwd+"/AE_lookup/AE_count_"+args.AE_num+"_"+datetime.today().strftime('%Y-%m-%d')+".csv",header=True,index=False) 

  aact_connect.close()

