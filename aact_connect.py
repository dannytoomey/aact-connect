import os, sys, psycopg, argparse, time
import pandas as pd
import pandas.io.sql as sqlio
from datetime import datetime
from tqdm import tqdm
import numpy as np
import codon

@codon.convert
class AACTConnect():

  __slots__ = 'cwd','args','aact_connect'
  
  def __init__(self,cwd,args):
    self.cwd = cwd
    self.args = args
    if os.path.exists(cwd+"/private/myconfig.txt"):
      with open(cwd+"/private/myconfig.txt") as file:
        credentials = file.readlines()
        user = str(credentials[0]).replace('\n','')
        password = str(credentials[1])
    else:
      parser.add_argument("-u", "--username", required=True)
      parser.add_argument("-p", "--password", required=True)
      user = str(self.args.username)
      password = str(self.args.password)

    self.aact_connect = psycopg.connect(
      host = "aact-db.ctti-clinicaltrials.org",
      user = user,
      password = password,
      dbname = "aact",
      port = 5432
    )

  def search(self):
    print("Performing query...")

    if os.path.exists(self.cwd+'/'+self.args.search):
      with open(self.cwd+'/'+self.args.search) as file:
        read_file = file.read()
      query = str(read_file).replace('\n',' ')
    else:
      self.aact_connect.close()
      sys.exit('Error: Please add your search query to the `query_text` directory as a .txt file')

    start = time.time()
    data = sqlio.read_sql_query(query,self.aact_connect) 
    query_frame = pd.DataFrame(data)
    query_frame.to_csv(self.cwd+"/query_results/query_results.csv",header=True,index=True)
    end = time.time()
    
    read_csv = pd.read_csv(self.cwd+"/query_results/query_results.csv")
    count = len(read_csv)
    
    if os.path.exists(self.cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv"):
      os.remove(self.cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv")
      os.rename(self.cwd+"/query_results/query_results.csv",self.cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv")
    else:
      os.rename(self.cwd+"/query_results/query_results.csv",self.cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv")

    last_search_rel = "/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv"
    last_search = self.cwd+"/query_results/query_"+str(count)+"_results_"+datetime.today().strftime('%Y-%m-%d')+".csv"

    print(f"Query returned {count} results in {end - start:01.2f} seconds. ")
    print(f"Results are logged at {last_search_rel}")

    if(self.args.add == None and self.args.lookup == None and self.args.AE_num == None):
      self.aact_connect.close()

    return count,last_search_rel

  @codon.jit(pyvars=['pd','np','tqdm','datetime','sqlio','os'])
  def add_results(self):
    
    print("Adding additional results...")

    if(self.args.use_search != None):
      query_frame = pd.read_csv(self.cwd+"/"+self.args.use_search)
    else:
      query_frame = pd.read_csv(self.cwd+self.args.last_search)

    split = len(query_frame)

    query_split = np.array_split(query_frame,split)
    result_concat_1 = pd.DataFrame()
    
    for sp in tqdm(query_split):
      
      nct_id = sp['nct_id'].to_list()[0]

      combined_query_1 = f'''
      select cv.nct_id, cv.number_of_nsae_subjects, cv.minimum_age_num, cv.maximum_age_num, 
             dg.design_groups,
             iv.interventions,
             oap.p_value,oac.ci_percent,
             srp.pmid,src.citation,
             pf.recruitment_details,
             rd.AE_Count,rd.SAE_Count,rd.Mortality_Count,
             re.Num_AEs_described

        from (
        select calculated_values.nct_id, calculated_values.number_of_nsae_subjects, calculated_values.minimum_age_num, calculated_values.maximum_age_num
        from calculated_values 
        where calculated_values.nct_id = '{nct_id}' ) as cv
        left join (
        select design_groups.nct_id, string_agg(design_groups.description,'; ') as design_groups
        from design_groups 
        group by design_groups.nct_id) as dg
        on cv.nct_id = dg.nct_id
        left join (
        select interventions.nct_id, string_agg(interventions.description,'; ') as interventions
        from interventions 
        group by interventions.nct_id) as iv
        on cv.nct_id = iv.nct_id
        left join (
        select outcome_analyses.nct_id, string_agg(CAST(outcome_analyses.p_value as VarChar),'; ') as p_value
        from outcome_analyses 
        group by outcome_analyses.nct_id) as oap
        on cv.nct_id = oap.nct_id
        left join (
        select outcome_analyses.nct_id, string_agg(CAST(outcome_analyses.ci_percent as VarChar),'; ') as ci_percent
        from outcome_analyses 
        group by outcome_analyses.nct_id) as oac
        on cv.nct_id = oac.nct_id
        left join (
        select study_references.nct_id, string_agg(CAST(study_references.pmid as VarChar),'; ') as pmid
        from study_references 
        group by study_references.nct_id) as srp
        on cv.nct_id = srp.nct_id
        left join (
        select study_references.nct_id, string_agg(CAST(study_references.citation as VarChar),'; ') as citation
        from study_references 
        group by study_references.nct_id) as src
        on cv.nct_id = src.nct_id
        left join (
        select participant_flows.nct_id, string_agg(CAST(participant_flows.recruitment_details as VarChar),'; ') as recruitment_details
        from participant_flows 
        group by participant_flows.nct_id) as pf
        on cv.nct_id = pf.nct_id
        left join (
        select reported_events.nct_id, COUNT(DISTINCT reported_events.adverse_event_term) AS Num_AEs_described
        from reported_events 
        group by reported_events.nct_id) as re
        on cv.nct_id = re.nct_id

        left join(
        select reported_event_totals.nct_id,
             sum(case when reported_event_totals.classification = 'Total, other adverse events' then 
                reported_event_totals.subjects_affected else 0 end) as AE_Count,
             sum(case when reported_event_totals.classification = 'Total, serious adverse events' then 
                reported_event_totals.subjects_affected else 0 end) as SAE_Count,
             sum(case when reported_event_totals.classification = 'Total, all-cause mortality' then 
                reported_event_totals.subjects_affected else 0 end) as Mortality_Count
        from reported_event_totals
        group by reported_event_totals.nct_id) as rd
        on cv.nct_id = rd.nct_id


      '''

      data_add_1 = sqlio.read_sql_query(combined_query_1,self.aact_connect)
      result_concat_1 = result_concat_1.append(data_add_1)

    new = query_frame.merge(result_concat_1,how='left',on='nct_id')

    new.to_csv(self.cwd+"/additional_data/query_additional_data.csv",header=True,index=False)

    read_csv = pd.read_csv(self.cwd+"/additional_data/query_additional_data.csv")

    count = len(read_csv)
    print("\nAdded additional data for "+str(count)+" results.")
    
    date=datetime.today().strftime('%Y-%m-%d')
        
    if os.path.exists(self.cwd+"/additional_data/query_"+str(count)+"_additional_data_"+date+".csv"):
      os.remove(self.cwd+"/additional_data/query_"+str(count)+"_additional_data_"+date+".csv")
      os.rename(self.cwd+"/additional_data/query_additional_data.csv",self.cwd+"/additional_data/query_"+str(count)+"_additional_data_"+date+".csv")
    else:
      os.rename(self.cwd+"/additional_data/query_additional_data.csv",self.cwd+"/additional_data/query_"+str(count)+"_additional_data_"+date+".csv")
    
    rel_path = "/additional_data/query_"+str(count)+"_additional_data_"+date+".csv"
    
    print(f"Results are logged at {rel_path}")

    self.aact_connect.close()

    return count,rel_path

  def lookup_AE(self):

    print("Looking up AE data for "+self.args.lookup+"...")

    query_lookup_1 = "select title, param_value, param_type from outcome_measurements where nct_id = '"+self.args.lookup+"' and (title ilike '%symptom%' or title ilike '%adverse%')"
    query_lookup_2 = "select classification, subjects_affected, subjects_at_risk from reported_event_totals where nct_id = '"+self.args.lookup+"'"
    query_lookup_3 = "select subjects_affected, subjects_at_risk, event_count, adverse_event_term from reported_events where nct_id = '"+self.args.lookup+"'"

    queries = [query_lookup_1,query_lookup_2,query_lookup_3]

    for query_add in queries:
      data_add = sqlio.read_sql_query(query_add,self.aact_connect)
      query_frame = pd.DataFrame(data_add)
      query_frame['nct_id'] = str(args.lookup)
      if query_add == queries[0]:
        data_concat = query_frame
      else:
        data_concat = data_concat.append(query_frame,ignore_index=True)
    
    data_concat.to_csv(self.cwd+"/AE_lookup/AE_lookup_"+self.args.lookup+"_"+datetime.today().strftime('%Y-%m-%d')+".csv",header=True,index=False)    

    print("Results are logged at /AE_lookup/AE_lookup_"+str(self.args.lookup+"_"+datetime.today().strftime('%Y-%m-%d'))+".csv")

    self.aact_connect.close()

  def get_AE_num(self):

    print("Couting numbers for AEs for "+self.args.AE_num+"...")
    
    query_lookup_1 = "select COUNT(DISTINCT adverse_event_term) AS Num_AEs_described from reported_events where nct_id = '"+self.args.AE_num+"'"
    
    data_add = sqlio.read_sql_query(query_lookup_1,self.aact_connect)
    query_frame = pd.DataFrame(data_add)
    query_frame['nct_id'] = str(self.args.AE_num)
    data_concat = query_frame

    data_concat.to_csv(self.cwd+"/AE_lookup/AE_count_"+self.args.AE_num+"_"+datetime.today().strftime('%Y-%m-%d')+".csv",header=True,index=False) 

    print("Results are logged at /AE_lookup/AE_count_"+str(self.args.AE_num+"_"+datetime.today().strftime('%Y-%m-%d'))+".csv")

    self.aact_connect.close()


if __name__ == '__main__':
  
  cwd = os.getcwd()
  parser = argparse.ArgumentParser()

  parser.add_argument("-s", "--search", required=False)
  parser.add_argument("-a", "--add", action="store_true", required=False)
  parser.add_argument("-us", "--use_search", required=False)
  parser.add_argument("-l", "--lookup", required=False)
  parser.add_argument("-ae", "--AE_num", required=False)

  args = parser.parse_args() 

  aact_connect = AACTConnect(cwd,args)

  if(args.search):
    results = aact_connect.search()
    if(args.add):
      args.last_search=results[1]
      aact_connect.add_results()
  elif(args.add):
    aact_connect.add_results()
  elif(args.lookup):
    aact_connect.lookup_AE()
  elif(args.AE_num):
    aact_connect.get_AE_num()

    
    
    


