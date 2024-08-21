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
        if calc.lower() in ('median'):
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].median()
        elif calc.lower() in ('average', 'mean'):
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].mean()
        elif calc.lower() in ('aggregate', 'sum'):
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].sum()
        elif calc.lower() in ('pct_chg', 'percent change', 'growth', 'mwr'):
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].sum().pct_change()
        elif calc.lower() in ('index', 'indexed', 'cumulative growth', 'twr'):
            #Update to handle NaN/0 across periods
            result = filtered_df.groupby('ReportPeriodEndDate')['Value'].sum().pct_change().add(1).cumprod()       
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


    def contentStats(self, item: str, by: str = None, as_percentage: bool = False):
        """
        Provides insight into the dataset by calculating the number or percentage of RSSD_ID or MDRM_Items.

        Parameters:
            item (str): The type of data to analyze ('RSSD_ID' or 'MDRM_Item').
            by (str): The dimension to analyze by ('ReportPeriodEndDate' or None).
            as_percentage (bool): Whether to return the results as percentages (default is False).
        
        Returns:
            pandas.DataFrame: A dataframe with the count or percentage of the specified item.
        """
        # Convert column names to lowercase for comparison
        columns_lower = {col.lower(): col for col in self.df.columns}

        # Convert input parameters to lowercase for case-insensitive comparison
        item = item.lower()
        by = by.lower() if by else None

        # Validate parameters
        if item not in columns_lower or (by and by not in columns_lower):
            print(f"Invalid 'item' or 'by' parameter. Available columns are: {list(self.df.columns)}")
            return

        # Map back to original case-sensitive column names
        item_col = columns_lower[item]
        by_col = columns_lower[by] if by else None

        # Process RSSD_ID
        if item == 'rssd_id':
            if by is None:
                # Total count or percentage of RSSD_IDs overall
                total_count = self.df[item_col].nunique()
                pvt = pd.DataFrame({item_col: [total_count]}, index=['Total'])
                
                if as_percentage:
                    pvt[item_col] = 100.0  # If it's a subtotal, it represents 100%

            elif by == 'reportperiodenddate':
                # Count or percentage of RSSD_IDs by ReportPeriodEndDate
                pvt = self.df[[by_col, item_col]].drop_duplicates().pivot_table(
                    index=by_col,
                    values=item_col,
                    aggfunc='count'
                ).sort_values(by=by_col, ascending=False)
                
                if as_percentage:
                    total_rssd_ids = pvt[item_col].sum()
                    pvt[item_col] = (pvt[item_col] / total_rssd_ids) * 100

        # Process MDRM_Item
        elif item == 'mdrm_item':
            if by is None:
                # Total count or percentage of MDRM_Items overall by RSSD_ID
                total_count = self.df[item_col].nunique()
                pvt = pd.DataFrame({item_col: [total_count]}, index=['Total'])
                
                if as_percentage:
                    pvt[item_col] = 100.0  # If it's a subtotal, it represents 100%

            elif by == 'reportperiodenddate':
                # Count or percentage of MDRM_Items by ReportPeriodEndDate
                pvt = self.df[[by_col, item_col]].drop_duplicates().pivot_table(
                    index=by_col,
                    values=item_col,
                    aggfunc='count'
                ).sort_values(by=by_col, ascending=False)
                
                if as_percentage:
                    total_items = pvt[item_col].sum()
                    pvt[item_col] = (pvt[item_col] / total_items) * 100

        else:
            print(f"Invalid 'item' parameter. Must be either 'RSSD_ID' or 'MDRM_Item'.")
            return

        pvt.fillna(0, inplace=True)
        #print(pvt)
        return pvt