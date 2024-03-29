import pandas as pd
from df_compare import DF_Compare

# **load files for the example**
# Large Files
left_df = pd.read_csv('demo_data/netflix_titles_orig.csv')
rigth_df = pd.read_csv('demo_data/netflix_titles_mod.csv')

exclude_col = ['system_time']
join_col = ['show_id']
comdf = DF_Compare(left_df, rigth_df, exclude_col, join_col)
compare_df = comdf.get_differences_by_deep_compare_join_columns()
# print(compare_df)
# compare_df = comdf.get_differences_by_compare_whole_row()
# htdf = df_extract_head_and_tail(compare_df)

# file_name = "test.xlsx"
# compare_df.to_excel(file_name, index=False, engine='openpyxl')