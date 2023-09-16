# -*- coding: utf-8 -*-
"""
Created on Wed sep 14 2023

@author: Alberto Almario
@email: albertoalmario@gmail.com
"""

import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType



class DF_Compare_spark():
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
        self.sort_columns_dataframe()
        if self.print_logs:
            print('Initial Shape(rows, columns): L->', self.df_shape(self.df_left), '| R->', self.df_shape(self.df_right))
        self.__remove_exclude_columns()
        self.__set_same_data_types()
        if self.lr_strip:
            self.__remove_whitespace()

    def df_shape(self, data_frame):    
        df_shape = (data_frame.count(), len(data_frame.columns))
        return df_shape

    def refresh_column_list(self):
        self.left_columns = sorted(self.df_left.columns)
        self.right_columns = sorted(self.df_right.columns)

    def sort_columns_dataframe(self):
        self.df_left = self.df_left.select(*(sorted(self.df_left.columns)))
        self.df_right = self.df_right.select(*(sorted(self.df_right.columns)))
        
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
            self.df_left = self.df_left.drop(*self.columns_to_exclude)
            self.df_right = self.df_right.drop(*self.columns_to_exclude)
            self.refresh_column_list()
            if self.print_logs:
                print('Shape(rows, columns) after delete columns to exclude: L->', self.df_shape(self.df_left), '| R->' , self.df_shape(self.df_right))
    
    def __set_same_data_types(self):
        """
        Set the same data types for the two DataFrames
        """

        left_dtypes = {field.name: field.dataType for field in self.df_left.schema.fields}
        right_dtypes = {field.name: field.dataType for field in self.df_right.schema.fields}

        mismatched_columns = [col_name for col_name, dtype in left_dtypes.items() if dtype != right_dtypes[col_name]]

        for col_name in mismatched_columns:
            if str(left_dtypes[col_name]) != "StringType":
                self.df_left = self.df_left.withColumn(col_name, F.col(col_name).cast("string"))

            if str(right_dtypes[col_name]) != "StringType":
                self.df_right = self.df_right.withColumn(col_name, F.col(col_name).cast("string"))

    def __remove_whitespace(self):
        """
        Remove extra leading and tailing whitespace from the DataFrames
        """
        
        # iterating over the columns from left DataFrame
        for col_name in self.df_left.columns:
            # checking datatype of each columns
            if isinstance(self.df_left.schema[col_name].dataType, StringType):
                # applying strip function on column
                self.df_left = self.df_left.withColumn(col_name, F.trim(F.col(col_name)))
        
        # iterating over the columns from right DataFrame
        for col_name in self.df_right.columns:
            # checking datatype of each columns
            if isinstance(self.df_right.schema[col_name].dataType, StringType):
                # applying strip function on column
                self.df_right = self.df_right.withColumn(col_name, F.trim(F.col(col_name)))
      
    def find_duplicate_rows_by_whole_row(self):
        """
        Find duplicated rows by whole rows, return two DataFrames.
        """
        def find_dups(df):
            # Group by all columns and filter duplicate rows
            return (df.groupBy(df.columns)
                    .agg(F.count("*").alias("cnt"))
                    .filter("`cnt` > 1")
                    .drop("cnt"))

        left_dup_rows = None
        right_dup_rows = None

        left_dup_rows = find_dups(self.df_left)
        right_dup_rows = find_dups(self.df_right)

        return left_dup_rows, right_dup_rows
 
    def find_duplicate_rows_by_join_columns(self):
        """
        Find duplicated rows by join columns, return two DataFrames.
        """

        def find_dups(df, columns):
            # Group by all columns and filter duplicate rows
            return (df.groupBy(columns)
                    .agg(F.count("*").alias("cnt"))
                    .filter("`cnt` > 1")
                    .drop("cnt"))

        left_dup_rows = None
        right_dup_rows = None

        if len(self.columns_to_join) >= 1:
            left_dup_rows = find_dups(self.df_left, self.columns_to_join)
            right_dup_rows = find_dups(self.df_right, self.columns_to_join)
        else:
            if self.print_logs:
                print('Not provided columns to join')

        return left_dup_rows, right_dup_rows
 
    def get_differences_by_compare_whole_row(self):
        """
        Compare the two DataFrames by the whole row and return DataFrame with differences.
        If the return is None, that means two DataFrame match by whole row
        """
        hash_left = F.sha2(F.concat_ws("", *self.df_left.columns), 256)
        hash_right = F.sha2(F.concat_ws("", *self.df_right.columns), 256)
    
        df_left_hashed = self.df_left.withColumn('hashed_columns', hash_left)
        df_right_hashed = self.df_right.withColumn('hashed_columns', hash_right)

        # Filter differences only
        df_left_only = (df_left_hashed.join(df_right_hashed, df_left_hashed.hashed_columns == df_right_hashed.hashed_columns, how="leftanti")).withColumn('diff_status', F.lit('left_only'))
        df_right_only = (df_right_hashed.join(df_left_hashed, df_right_hashed.hashed_columns == df_left_hashed.hashed_columns, how="leftanti")).withColumn('diff_status', F.lit('right_only'))

        differences = df_left_only.union(df_right_only)
        differences = differences.drop('hashed_columns')

        # Counting the number of left_only and right_only rows.
        rows_left_only = differences.filter(F.col('diff_status') == 'left_only').count()
        rows_right_only = differences.filter(F.col('diff_status') == 'right_only').count()

        if rows_left_only > 0 or rows_right_only > 0:
            if self.print_logs:
                print('Compare by Whole Row using hash: Exist Left only->', rows_left_only, '|Exist Right only->', rows_right_only)
            return differences
        else:
            if self.print_logs:
                print('The two DataFrames match by whole row using hash')
            return None


    
    def get_differences_by_deep_compare_join_columns(self):
        """
        Compare the two Spark DataFrames by join columns, and check cell by cell. 
        Return DataFrame with differences.
        This method could be delayed if there are many rows to compare.
        If the return is None, that means two DataFrames match by Join Columns and compare cell by cell.
        """
        if not self.columns_to_join:
            raise Exception('Not provided columns to join')
        
        # remove rows that mach by whole row
        df_whole_row_comp = self.get_differences_by_compare_whole_row()
        if df_whole_row_comp is None:
            if self.print_logs:
                print('The two DataFrames do not have differences')
            return None
        
        # Filter left_only and right_only rows
        df_only_left = df_whole_row_comp.filter(F.col("diff_status") == "left_only")
        df_only_right = df_whole_row_comp.filter(F.col("diff_status") == "right_only")

        def compare_join_columns(col_name):
            left_col = F.coalesce(F.col(f"df_left.{col_name}"), F.lit('NULL'))
            right_col = F.coalesce(F.col(f"df_right.{col_name}"), F.lit('NULL'))

            return F.when(
                            left_col != right_col,
                            F.concat_ws(' != ', left_col, right_col )
                        ).otherwise(left_col)

        def compare_others_columns(col_name):
            left_col = F.coalesce(F.col(f"df_left.{col_name}"), F.lit('NULL'))
            right_col = F.coalesce(F.col(f"df_right.{col_name}"), F.lit('NULL'))

            return F.when(
                            left_col != right_col,
                            F.concat_ws(' != ', left_col, right_col )
                        ).otherwise(F.lit(""))

        # Join the two dataframes by the columns define to do join
        joined = df_only_left.alias("df_left").join(df_only_right.alias("df_right"), 
                                                on=[F.col("df_left." + col) == F.col("df_right." + col) for col in self.columns_to_join], 
                                                how="outer")
        
        # the function of compare for every column
        comparison_join_cols = {col: compare_join_columns(col).alias(col) for col in self.columns_to_join}

        other_col_list = [x for x in self.left_columns if x not in self.columns_to_join]
        comparison_other_cols = {col: compare_others_columns(col).alias(col) for col in other_col_list}

        # final dataframe
        df_with_differences = joined.select(*list(comparison_join_cols.values()), *list(comparison_other_cols.values()))
        
        return df_with_differences
        


if __name__ == '__main__':
    pass

