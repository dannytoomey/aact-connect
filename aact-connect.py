import os, sys, psycopg2, argparse
import pandas as pd
import pandas.io.sql as sqlio
from functools import reduce
import progressbar, time

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--username", required=True)
parser.add_argument("-p", "--password", required=True)
parser.add_argument("-s", "--search", action="store_true", required=False)
parser.add_argument("-a", "--add", action="store_true", required=False)
parser.add_argument("-l", "--lookup", required=False)
args = parser.parse_args()

aact_connect = psycopg2.connect(
	host = "aact-db.ctti-clinicaltrials.org",
	user = str(args.username),
	password = str(args.password),
	database = "aact",
	port = 5432
)

cwd = os.getcwd()

if args.search:

  print("Performing query...")

  cursor = aact_connect.cursor()

  query = '''

  select * from studies WHERE
   (official_title ilike '%challenge%') or 
   (official_title ilike '%immunization%' and official_title ilike '%sporozoites%') or 
   (official_title ilike '%human%' and official_title ilike '%carriage%') or 
   (official_title ilike '%infection%' and 
     		(official_title ilike '%controlled%' or official_title ilike '%experimental%' or 
            official_title ilike '%induced%')) or 
   (official_title ilike '%efficacy%' and official_title ilike '%vaccine%') or
   (official_title ilike '%human%' and official_title ilike '%exposure%') 
    OR
   (brief_title ilike '%challenge%') or 
   (brief_title ilike '%immunization%' and brief_title ilike '%sporozoites%') or 
   (brief_title ilike '%human%' and brief_title ilike '%carriage%') or 
   (brief_title ilike '%infection%' and 
     	(brief_title ilike '%controlled%' or brief_title ilike '%experimental%' or 
            brief_title ilike '%induced%')) or 
   (brief_title ilike '%efficacy%' and brief_title ilike '%vaccine%') or
   (brief_title ilike '%human%' and brief_title ilike '%exposure%') 
    OR
   (acronym ilike '%challenge%') or
   (acronym ilike '%human%')


  '''

  data = sqlio.read_sql_query(query,aact_connect)
  query_frame = pd.DataFrame(data)
  query_frame.to_csv(cwd+"/query_results.csv",header=True,index=True)


if args.add:

  print("Adding additional results...")

  nct_id_array = query_frame['nct_id'].to_list()
  #nct_id_array = ['NCT00931892','NCT04469816','NCT00340574','NCT01111305','NCT00002058']

  widgets = [' [',progressbar.Timer(format= 'elapsed time: %(elapsed)s'),'] ',progressbar.Bar('*'),' (',progressbar.ETA(), ') ',]
  bar = progressbar.ProgressBar(max_value=200,widgets=widgets).start()

  for nct_id in nct_id_array:

    #print(nct_id)

    trial = pd.DataFrame(query_frame.loc[query_frame['nct_id'] == nct_id])

    query_add_1 = "select number_of_nsae_subjects from calculated_values where nct_id = '"+nct_id+"'"
    query_add_2 = "select minimum_age_num from calculated_values where nct_id = '"+nct_id+"'"
    query_add_3 = "select maximum_age_num from calculated_values where nct_id = '"+nct_id+"'"
    query_add_4 = "select recruitment_details from participant_flows where nct_id = '"+nct_id+"'"
    query_add_5 = "select ci_percent, p_value from outcome_analyses where nct_id = '"+nct_id+"'"
    query_add_6 = "select description from design_groups where nct_id = '"+nct_id+"'"
    query_add_7 = "select description from interventions where nct_id = '"+nct_id+"'"
    query_add_8 = "select pmid, citation from study_references where nct_id = '"+nct_id+"'"

    '''

    
    queries = [query_add_1,query_add_2,query_add_3,query_add_4,query_add_5,query_add_6,query_add_7,query_add_8]

    for query_add in queries:
      #print(query_add)
      data_add = sqlio.read_sql_query(query_add,aact_connect)
      query_frame = pd.DataFrame(data_add)
      query_frame['nct_id'] = nct_id
      query_frame = query_frame.iloc[:1]
      if query_add == queries[0]:
        data_concat = trial.merge(query_frame,on="nct_id",how="left")
      #else:
        #data_concat = data_concat.merge(query_frame,on="nct_id",how="left")

    '''

    
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

    

    if nct_id == nct_id_array[0]:
      result_concat = data_concat
      
    else:
      result_concat = data_concat.append(result_concat,ignore_index=True)
      
    bar.update((nct_id_array.index(nct_id)/len(nct_id_array))*200)

  result_concat.to_csv(cwd+"/results_additional_data.csv",header=True,index=False)



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
  
  data_concat.to_csv(cwd+"/AE_lookup_"+args.lookup+".csv",header=True,index=False)


  




