# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 08:23:19 2021

@author: Alberto Almario
@email: albertoalmario@gmail.com
"""

import sys
import pandas as pd
from pandas.core.frame import DataFrame
from tqdm import tqdm



class DF_Compare():
    """
    Compare two DataFrames, it is necesary that the two DataFrames have de same columns names and quantity,
    not is necesary that have the same order columns
    parameter:
        df_left: Left DataFrame
        df_right: Right DataFrame
        columns_to_exclude: list of columns name to exclude of the compare process
        columns_to_join: list of columns names that will be use to do join between the two DataFrames
        print_logs: indicate if the process print logs on console, by default is True
        lr_strip: Remove spaces at the beginning and at the end of the string, by default is False
    """
    # agregar case sensitive true o false compare por defecto siempre es case sensitive

    def __init__(self, df_left, df_right, columns_to_exclude=[], columns_to_join=[], print_logs=True, lr_strip=False):
        self.df_left = df_left
        self.df_right = df_right
        self.columns_to_exclude = columns_to_exclude
        self.columns_to_join = columns_to_join
        self.print_logs = print_logs
        self.lr_strip = lr_strip
        self.left_columns = []
        self.right_columns = []
        self.__df_validations()
        if self.print_logs:
            print('Initial Shape(rows, columns): L->', self.df_left.shape, '| R->', self.df_right.shape)
        self.__remove_exclude_columns()
        self.__set_same_data_types()
        if self.lr_strip:
            self.__remove_whitespace()
        
    def refresh_column_list(self):
        self.left_columns = sorted(list(self.df_left.columns))
        self.right_columns = sorted(list(self.df_right.columns))
        
    def __df_validations(self):
        """
        Apply validations of input parameters
        """
        # Check instances of parameters
        if not isinstance(self.columns_to_join,list):
            validation_msg = "*** The parameter columns_to_join, not is a list examples: ['col1','col2'] or ['col1'] ***"
            raise Exception(validation_msg)
        if not isinstance(self.columns_to_exclude,list):
            validation_msg = "*** The parameter columns_to_exclude, not is a list examples: ['col1','col2'] or ['col1'] ***"
            raise Exception(validation_msg)
        if not isinstance(self.df_left,DataFrame):
            validation_msg = "*** The parameter df_left, not is a DataFrame ***"
            raise Exception(validation_msg)
        if not isinstance(self.df_right,DataFrame):
            validation_msg = "*** The parameter df_right, not is a DataFrame ***"
            raise Exception(validation_msg)
        ## Refresh columns list of DataFrames
        self.refresh_column_list()
        # Check if the columns are equals    
        if self.left_columns != self.right_columns:
            validation_msg = f"""*** The two DataFrames not have the same columns (names or quantity) ***
            \n Columns on left entity -> {self.left_columns} 
            \n Columns on right entity -> {self.right_columns}
            """
            raise Exception(validation_msg)
        # Check if the left DataFrame contain duplciate columns
        if len(self.left_columns) != len(set(self.left_columns)):
            validation_msg = "*** The left DataFrame contain duplicate columns ***"
            raise Exception(validation_msg)
        # Check if the right DataFrame contain duplciate columns
        if len(self.right_columns) != len(set(self.right_columns)):
            validation_msg = "*** The right DataFrame contain duplicate columns ***"
            raise Exception(validation_msg)
        # Check if the columns_to_exclude exist on DataFrames
        if len(self.columns_to_exclude) >= 1:
            if not set(self.columns_to_exclude).issubset(set(self.left_columns)):
                validation_msg = f"""*** The given columns to exclude not exist on two DataFrames ***
                \n Columns to exclude -> {self.columns_to_exclude} 
                """
                raise Exception(validation_msg)
        # Check if the columns_to_join exist on DataFrames
        if len(self.columns_to_join) >= 1:
            if not set(self.columns_to_join).issubset(set(self.left_columns)):
                validation_msg = f"""*** The given columns to join not exist on two DataFrames ***
                \n Columns to join -> {self.columns_to_join} 
                """
                raise Exception(validation_msg)
        
    def __remove_exclude_columns(self):
        """
        remove columns from the DataFrame
        """
        if len(self.columns_to_exclude) >= 1:
            self.df_left = self.df_left.drop(self.columns_to_exclude, axis=1)
            self.df_right = self.df_right.drop(self.columns_to_exclude, axis=1)
            self.refresh_column_list()
            if self.print_logs:
                print('Shape(rows, columns) after delete columns to exclude: L->', self.df_left.shape, '| R->' ,self.df_right.shape)
    
    def __set_same_data_types(self):
        """
        Set the same data types for the two DataFrames
        """
        # lr = Left Right
        lr_dtypes = pd.concat([self.df_left.dtypes, self.df_right.dtypes], axis=1)
        lr_dtypes = lr_dtypes[lr_dtypes[0] != lr_dtypes[1]]
        for idx, row in lr_dtypes.iterrows():
            if row[0] != 'object':
                self.df_left[idx] = self.df_left[idx].astype('object')
            if row[1] != 'object':
                self.df_right[idx] = self.df_right[idx].astype('object') 
    
    def __remove_whitespace(self):
        """
        Remove extra leading and tailing whitespace from the DataFrames
        """
        # iterating over the columns from left DataFrame
        for i in self.df_left.columns:
            # checking datatype of each columns
            if self.df_left[i].dtype == 'object':
                # applying strip function on column
                self.df_left[i] = self.df_left[i].astype(str).str.strip()
        
        # iterating over the columns from right DataFrame
        for i in self.df_right.columns:
            # checking datatype of each columns
            if self.df_right[i].dtype == 'object':
                # applying strip function on column
                self.df_right[i] = self.df_right[i].astype(str).str.strip()
                
    
    def find_duplicate_rows_by_whole_row(self):
        """
        Find duplicated rows by whole rows, retrun two DataFrame.
        """
        left_dup_rows = None
        right_dup_rows = None
        # find in left data frame
        left_dup_rows = self.df_left[self.df_left.duplicated()]
        # find in right data frame
        right_dup_rows = self.df_right[self.df_right.duplicated()]
        return left_dup_rows, right_dup_rows  
    
    def find_duplicate_rows_by_join_columns(self):
        """
        Find duplicated rows by join columns, retrun two DataFrame.
        """
        left_dup_rows = None
        right_dup_rows = None
        if len(self.columns_to_join) >= 1:
            # find in left data frame
            ldf = self.df_left[self.columns_to_join]
            left_dup_rows = ldf[ldf.duplicated()]
            # find in right data frame
            rdf = self.df_right[self.columns_to_join]
            right_dup_rows = rdf[rdf.duplicated()]
            return left_dup_rows, right_dup_rows  
        else:
            if self.print_logs:
                print('Not provided columns to join')
        return left_dup_rows, right_dup_rows  
                
    def get_differences_by_compare_whole_row(self):
        """
        Compare the two DataFrames by the whole row and return DataFrame with differences.
        if the return is None, that mean two DataFrame match by whole row
        """
        df_differences = None
        outer_join = self.df_left.merge(self.df_right
                       ,how='outer' 
                       ,left_on=self.left_columns
                       ,right_on=self.right_columns
                       ,suffixes=('_left_df', '_right_df')
                       ,indicator=True
                       )
        outer_join = outer_join[outer_join['_merge'] != 'both']
    
        if len(outer_join) >= 1:
            df_differences = outer_join
            if self.print_logs:
                rows_left_only = len(outer_join[outer_join['_merge'] != 'left_only'])
                rows_right_only = len(outer_join[outer_join['_merge'] != 'right_only'])
                print('Compare by Whole Row: Exsist Left only->', rows_left_only , '|Exsist Right only->' , rows_right_only)
        else:
            if self.print_logs:
                print('The two DataFrames match by whole row')
        
        return df_differences
    
    def get_differences_by_compare_join_columns(self):
        """
        Compare the two DataFrames by join columns and return DataFrame with differences.
        if the return is None, that mean two DataFrame match by Join Columns
        """
        df_differences = None        
        if len(self.columns_to_join) >= 1:
            outer_join = self.df_left.merge(self.df_right
                           ,how='outer' 
                           ,left_on=self.columns_to_join
                           ,right_on=self.columns_to_join
                           ,suffixes=('_left_df', '_right_df')
                           ,indicator=True
                           )
            outer_join = outer_join[outer_join['_merge'] != 'both']
            
            if len(outer_join) >= 1:
                df_differences = outer_join
                if self.print_logs:
                    rows_left_only = len(outer_join[outer_join['_merge'] != 'left_only'])
                    rows_right_only = len(outer_join[outer_join['_merge'] != 'right_only'])
                    print('Compare by Join Columns: Exsist Left only->', rows_left_only , '|Exsist Right only->' , rows_right_only)
            else:
                if self.print_logs:
                    print('The two DataFrames match by Join Columns')
        else:
            raise Exception ('Not provided columns to join')
                
        return df_differences
    
    def get_differences_by_deep_compare_join_columns(self):
        """
        Compare the two DataFrames by join columns, and check cell by cell. 
        Return DataFrame with differences.
        This method could be delayed if there are many rows to compare
        if the return is None, that mean two DataFrame match by Join Columns and compare cell by cell
        """
        df_differences = None
        if len(self.columns_to_join) >= 1:
            suffix_left = '_left_df'
            suffix_right = '_right_df'
            
            
            # Delete coincident rows on two DataFrames and get left_only and right_only
            # this to compare only the not matched by whole row and do the process lighter 
            print_logs_orig_value = self.print_logs
            self.print_logs = False
            df_whole_row_comp = self.get_differences_by_compare_whole_row()
            self.print_logs = print_logs_orig_value
            
            # If the compare by whole row not generate differences finished the process
            if df_whole_row_comp is None:
                if self.print_logs:
                    print('The two DataFrames Not have differences')
                return df_whole_row_comp
            
            df_only_left = df_whole_row_comp[df_whole_row_comp['_merge']=='left_only']
            df_only_left = df_only_left.drop('_merge', axis=1)
            df_only_right = df_whole_row_comp[df_whole_row_comp['_merge']=='right_only']
            df_only_right = df_only_right.drop('_merge', axis=1)
            
            # Outer join by join columns
            outer_join = df_only_left.merge(df_only_right
                               ,how='outer' 
                               ,left_on=self.columns_to_join
                               ,right_on=self.columns_to_join
                               ,suffixes=(suffix_left, suffix_right)
                               ,indicator=True
                               )
            
            columns = []
            for col_name in list(outer_join.columns): # Get unique names whitout suffixes
                columns.append(col_name.replace(suffix_left,'').replace(suffix_right,''))
            columns.remove('_merge')
            columns = list(dict.fromkeys(columns)) # Delete duplicates
            df_differences = pd.DataFrame(columns=columns) # Create empty DataFrame
            
            # Create new progress bar
            pbar = tqdm(desc='finding differences', total=len(outer_join), position=0, leave=True, file=sys.stdout)
            # Compare field by field of matched rows by Code
            try:
                idx = 0
                for idx, row in outer_join.iterrows():
                    pbar.update(1)
                    tmp_row = []
                    for col in columns:
                        if col in self.columns_to_join:
                            if row['_merge'] == 'both':
                                tmp_row.append(row[col])
                            elif row['_merge'] == 'left_only':
                                tmp_row.append(str(row[col]) + ' != None')
                            elif row['_merge'] == 'right_only':
                                tmp_row.append('None != ' + str(row[col]))
                            continue
                        col_left = col + suffix_left
                        col_right = col + suffix_right
                        row_col_left = str(row[col_left])
                        row_col_right = str(row[col_right])
                        if row_col_left != row_col_right: # Compare the field Left Vs the field Right
                            tmp_row.append(row_col_left + ' != ' +  row_col_right)
                        else:
                            tmp_row.append('')
                    df_differences.loc[idx] = tmp_row # Append temporal row to the response DataFrame
                    idx += 1
            except Exception as e:
                raise Exception(e)
            finally:
                # Close progress bar
                pbar.close()
                # pass
                
            if len(df_differences) >= 1:
                if self.print_logs:
                    print('The two DataFrames present differencess')
            else:
                if self.print_logs:
                    print('The two DataFrames Not have differences')
        else:
            raise Exception ('Not provided columns to join')
            
        return df_differences
        
def df_extract_head_and_tail(data_frame, num_rows=100):
    df_head_tail = data_frame.head(num_rows)
    df_head_tail = df_head_tail.append(data_frame.tail(num_rows))
    return df_head_tail



if __name__ == '__main__':
    pass

