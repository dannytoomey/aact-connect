import os, sys, argparse, time
cwd = os.getcwd()
sys.path.append(cwd)

import pandas as pd
from aact_connect import AACTConnect
from datetime import datetime
from tqdm import tqdm

class TestAACT():
	def __init__(self,cwd,args,aact_connect,df1,df2):
		self.cwd = cwd
		self.args = args
		self.aact_connect = aact_connect
		self.df1 = df1
		self.df2 = df2

	def test_search_number(self):
		print("\n --- --- --- --- --- ---")
		print('\nTesting that the number of results are the same as the 4/15/23 search...')
		self.results = self.aact_connect.search()
		try:
			assert self.results[0] == 5131
		except:
			print("Comparison search does not return the same number of results as the 4/15/23 search")
		else:
			print("Test passed!")
		print(" --- --- --- --- --- ---")

	def test_search_rows(self):
		if self.args.compare_current_frame:
			self.df2 = pd.read_csv(self.cwd+self.results[1])

		print("\n --- --- --- --- --- ---")
		print('\nChecking that the comparison search does not omit results from the 4/15/23 search...')
		count = 0
		for index, row in self.df1.iterrows():
			try:
				assert row['nct_id'] in self.df2['nct_id'].values
			except:
				print("Comparison search omits "+row['nct_id']+" present in 4/15/23 search")
				count += 1
		if count == 0:
			print("Test passed!")

		print('\nChecking that the 4/15/23 search does not omit results from the comparison search...')
		count = 0
		for index, row in self.df2.iterrows():
			try:
				assert row['nct_id'] in self.df1['nct_id'].values
			except:
				print("4/15/23 search omits "+row['nct_id']+" present in comparison search")
				count += 1
		if count == 0:
			print("Test passed!")
		
		print(" --- --- --- --- --- ---")

	def test_search_columns(self):
		if self.args.compare_current_frame:
			self.args.use_search = self.results[1]
			res = self.aact_connect.add_results()
			self.df2 = pd.read_csv(self.cwd+res[1])
		print(" --- --- --- --- --- ---")
		print('\nComparing all column data from the current search to the 4/15/23 search...')
		change_df = []

		df1_cols = self.df1.columns.tolist()
		df2_cols = self.df2.columns.tolist()
		error_count = 0 

		for col in df2_cols:
			try: 
				assert col in df1_cols
			except:
				print(str("Column '"+col+"' in current search not present in 4/15/23 search"))
				error_count += 1
		for col in df1_cols:
			try: 
				assert col in df2_cols
			except:
				print(str("Column '"+col+"' in 4/15/23 search not present in current search"))
				error_count += 1

		self.df1.fillna('Empty',inplace=True)
		self.df2.fillna('Empty',inplace=True)
		
		if(error_count == 0):
			for index, row in tqdm(self.df1.iterrows()):
				df1_row = self.df1.loc[self.df1['nct_id']==row['nct_id']]
				df2_row = self.df2.loc[self.df2['nct_id']==row['nct_id']]
				try:
					assert(len(df1_row.columns.tolist()) == len(df2_row.columns.tolist()))
				except:
					print("Error: Data frames do not have the same number of columns")
					break
				else:
					num_cols = len(df1_row.columns.tolist())
				for col in range(num_cols):
					col_name = df1.columns.tolist()[col]
					if(df1_row[col_name].name=='Unnamed: 0' or 
					   df1_row[col_name].name=='updated_at' or 
					   df1_row[col_name].name=='created_at'):
						pass
					else:
						if(df1_row[col_name].values[0] == df2_row[col_name].values[0]):
							pass
						else:
							this_change = {'nct_id':df1_row['nct_id'].values[0],
										   'column':df1_row[col_name].name,
										   '4/15/23 value':df1_row[col_name].values[0],
										   'Current search value':df2_row[col_name].values[0]
										   }
							change_df.append(this_change)
		
		
		change_df = pd.DataFrame(change_df)
		change_df.to_csv(cwd+"/tests/CT_SR_search_comparisons_"+datetime.today().strftime('%Y-%m-%d')+".csv")
		print(""+str(len(change_df))+" changed logged at /tests/CT_SR_search_comparisons_"+str(datetime.today().strftime('%Y-%m-%d'))+".csv")
		
	def run_tests(self):
		if self.args.compare_current_frame:
			self.test_search_number()
			self.test_search_rows()
			self.test_search_columns()
		if self.args.compare_existing_frame:
			self.test_search_rows()
			self.test_search_columns()

if __name__=='__main__':

	parser = argparse.ArgumentParser()
	cwd = os.getcwd()
	
	parser.add_argument("-s", "--search", required=False)
	parser.add_argument("-a", "--add", action="store_true", required=False)
	parser.add_argument("-us", "--use_search", required=False)
	parser.add_argument("-l", "--lookup", required=False)
	parser.add_argument("-ae", "--AE_num", required=False)
	parser.add_argument("-cef", "--compare_existing_frame",required=False)
	parser.add_argument("-ccf", "--compare_current_frame", action="store_true", required=False)
		
	args = parser.parse_args()

	df1 = pd.read_csv(cwd+"/additional_data/query_5131_additional_data_2023-04-15.csv")
	if args.compare_existing_frame != None:
		df2 = pd.read_csv(cwd+"/"+args.compare_existing_frame)
	if args.compare_current_frame:
		df2 = None

	aact_connect = AACTConnect(cwd,args)
	tests = TestAACT(cwd,args,aact_connect,df1,df2)
	tests.run_tests()

