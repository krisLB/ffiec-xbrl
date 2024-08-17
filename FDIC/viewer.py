from FDIC import constants as paths

import pandas as pd
import matplotlib.pyplot as plt

class Viewer:
    def __init__(self, path=paths.localPath + paths.filename_MasterCall, df=None):
        self.path = None
        self.df = None
        if path:
            self.load(path=path)
        elif df is not None:
            self.load(df=df)
    
    def load(self, path=None, df=None):
        if path:
            try:
                self.df = pd.read_csv(path)
                self.path = path
            except Exception as e:
                print(f"Error loading file: {e}")
        elif df is not None:
            self.df = df
    
    def query(self, MDRMitem=None, RSSDid=None, ReportPeriodEndDate=None, calc='aggregate', chart=False):
        if self.df is None:
            print("Dataframe is not loaded.")
            return
        
        filtered_df = self.df.copy()
        
        # Apply filters
        if MDRMitem is not None:
            if isinstance(MDRMitem, list):
                filtered_df = filtered_df[filtered_df['MDRM_Item'].isin(MDRMitem)]
            else:
                filtered_df = filtered_df[filtered_df['MDRM_Item'] == MDRMitem]
        
        if RSSDid is not None:
            if isinstance(RSSDid, list):
                filtered_df = filtered_df[filtered_df['RSSD_ID'].isin(RSSDid)]
            else:
                filtered_df = filtered_df[filtered_df['RSSD_ID'] == RSSDid]
        
        if ReportPeriodEndDate is not None:
            if isinstance(ReportPeriodEndDate, list):
                filtered_df = filtered_df[filtered_df['ReportPeriodEndDate'].isin(ReportPeriodEndDate)]
            else:
                filtered_df = filtered_df[filtered_df['ReportPeriodEndDate'] == ReportPeriodEndDate]
        
        # Cast 'Value' column to appropriate data type based on 'MDRM_Item'
        ##UPDATE THIS - move out here and develop process around this
        item_type_map = {
            'RCON2170': int,
            'RCOA5311': int,
            'RCOA7204': float,
            'RCOA7205': float,
            'RCOA7206': float,  
        }
        value_type = item_type_map[MDRMitem]
        if MDRMitem and isinstance(MDRMitem, str) and MDRMitem in item_type_map:
            filtered_df['Value'] = filtered_df['Value'].astype(item_type_map[MDRMitem])
        elif MDRMitem and isinstance(MDRMitem, list):
            for item in MDRMitem:
                if item in item_type_map:
                    filtered_df.loc[filtered_df['MDRM_Item'] == item, 'Value'] = filtered_df.loc[filtered_df['MDRM_Item'] == item, 'Value'].astype(item_type_map[item])
        
        # Perform calculation
        if calc.lower() == 'median':
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].median()
        elif calc.lower() == ('average' or 'mean'):
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].mean()
        elif calc.lower() == ('aggregate' or 'sum'):
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].sum()
        else:
            print(f'Calculation type "{calc}" not supported')
            return

        # Format results
        if value_type == float:
            formatted_result = result.map(lambda x: f"{x:,.2f}")
        elif value_type == int:
            formatted_result = result.map(lambda x: f"{x:,}")
        else:
            formatted_result = result

        # Display chart if requested
        if chart:
            result.plot(kind='line', title='Result over Time')
            plt.xlabel('Report Period End Date')
            plt.ylabel('Value')
            plt.grid(True)
            plt.show()
        
        return formatted_result



