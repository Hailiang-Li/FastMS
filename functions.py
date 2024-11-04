import pandas as pd
import numpy as np


class No_MS2_Processor:
    def __init__(self, area_columns, mz_threshold=1.5, rt_threshold=2.5, ms_threshold=1.5):
        self.area_columns = area_columns
        self.mz_threshold = mz_threshold
        self.rt_threshold = rt_threshold
        self.ms_threshold = ms_threshold


    def merge_rows(self, group):
        ms2_group = group[group['MS2'] != 'No MS2']
        no_ms2_group = group[group['MS2'] == 'No MS2']
        merged_data = []
        used_indices = set()

        for i in range(len(no_ms2_group)):
            if i in used_indices:
                continue  # Skip already merged rows

            temp_group = [no_ms2_group.iloc[i]]
            used_indices.add(i)

            for j in range(i + 1, len(no_ms2_group)):
                if j in used_indices:
                    continue  # Skip already merged row

                row = no_ms2_group.iloc[j]
                if abs(row['m/z'] - temp_group[0]['m/z']) <= self.mz_threshold and abs(row['RT [min]'] - temp_group[0]['RT [min]']) <= self.rt_threshold:
                    temp_group.append(row)
                    used_indices.add(j)

            if len(temp_group) > 1:
                merged_row = temp_group[0].copy()
                for temp_row in temp_group[1:]:
                    for col in self.area_columns:
                        merged_row[col] = max(merged_row[col], temp_row[col])
                merged_row['RT [min]'] = sum(r['RT [min]'] for r in temp_group) / len(temp_group)
                merged_data.append(merged_row)
            else:
                merged_data.append(temp_group[0])

        merged_data_df = pd.DataFrame(merged_data)
        final_group = pd.concat([ms2_group, merged_data_df], ignore_index=True)
        return final_group.drop_duplicates()
    

    def filter_rows(self, group):
        filtered_group = group[abs(group['Calc. MW'] - group['m/z']) <= self.ms_threshold]
        return filtered_group
    

    def process_group(self, group):
        ms2_group = group[group['MS2'] != 'No MS2']
        no_ms2_group = group[group['MS2'] == 'No MS2']

        if no_ms2_group.empty:
            return group
        
        no_ms2_group['non_zero_count'] = (no_ms2_group[self.area_columns] != 0).sum(axis=1)
        max_non_zero_count = no_ms2_group['non_zero_count'].max()
        max_non_zero_rows = no_ms2_group[no_ms2_group['non_zero_count'] == max_non_zero_count]

        if len(max_non_zero_rows) > 1:
            max_non_zero_rows['sum_values'] = max_non_zero_rows[self.area_columns].sum(axis=1)
            max_row = max_non_zero_rows.loc[max_non_zero_rows['sum_values'].idxmax()]
        else:
            max_row = max_non_zero_rows.iloc[0]

        max_row = max_row.drop(['non_zero_count', 'sum_values'], errors='ignore')

        return pd.concat([ms2_group, pd.DataFrame([max_row])], ignore_index=True)
    

class MS2_DDA_Processor:
    def __init__(self, area_columns, mz_threshold=1.5, rt_threshold=2.5, ms_threshold=1.5):
        self.area_columns = area_columns
        self.mz_threshold = mz_threshold
        self.rt_threshold = rt_threshold
        self.ms_threshold = ms_threshold


    def merge_rows(self, group):
        ms2_group = group[group['MS2'] != 'No MS2']
        no_ms2_group = group[group['MS2'] == 'No MS2']
        merged_data = []
        used_indices = set()

        for i in range(len(ms2_group)):
            if i in used_indices:
                continue  # Skip already merged rows

            temp_group = [ms2_group.iloc[i]]
            used_indices.add(i)

            for j in range(i + 1, len(ms2_group)):
                if j in used_indices:
                    continue  # Skip already merged row

                row = ms2_group.iloc[j]
                if abs(row['m/z'] - temp_group[0]['m/z']) <= self.mz_threshold and abs(row['RT [min]'] - temp_group[0]['RT [min]']) <= self.rt_threshold:
                    temp_group.append(row)
                    used_indices.add(j)

            if len(temp_group) > 1:
                merged_row = temp_group[0].copy()
                for temp_row in temp_group[1:]:
                    for col in self.area_columns:
                        merged_row[col] = max(merged_row[col], temp_row[col])
                merged_row['RT [min]'] = sum(r['RT [min]'] for r in temp_group) / len(temp_group)
                merged_data.append(merged_row)
            else:
                merged_data.append(temp_group[0])

        merged_data_df = pd.DataFrame(merged_data)
        final_group = pd.concat([no_ms2_group, merged_data_df], ignore_index=True)
        return final_group.drop_duplicates()
    

    def filter_rows(self, group):
        filtered_group = group[abs(group['Calc. MW'] - group['m/z']) <= self.ms_threshold]
        return filtered_group
    

    def process_group(self, group):
        ms2_group = group[group['MS2'] != 'No MS2']
        no_ms2_group = group[group['MS2'] == 'No MS2']

        if ms2_group.empty:
            return group
        
        ms2_group['non_zero_count'] = (ms2_group[self.area_columns] != 0).sum(axis=1)
        max_non_zero_count = ms2_group['non_zero_count'].max()
        max_non_zero_rows = ms2_group[ms2_group['non_zero_count'] == max_non_zero_count]

        if len(max_non_zero_rows) > 1:
            max_non_zero_rows['sum_values'] = max_non_zero_rows[self.area_columns].sum(axis=1)
            max_row = max_non_zero_rows.loc[max_non_zero_rows['sum_values'].idxmax()]
        else:
            max_row = max_non_zero_rows.iloc[0]

        max_row = max_row.drop(['non_zero_count', 'sum_values'], errors='ignore')

        return pd.concat([no_ms2_group, pd.DataFrame([max_row])], ignore_index=True)
    

class MS2_Select_DataProcessor:
    def __init__(self, area_columns):
        self.area_columns = area_columns

    def process_group(self, group):
        if len(group) == 1:
            return group

        group['non_zero_count'] = (group[self.area_columns] != 0).sum(axis=1)
        max_non_zero_count = group['non_zero_count'].max()
        max_non_zero_rows = group[group['non_zero_count'] == max_non_zero_count]

        if len(max_non_zero_rows) > 1:
            max_non_zero_rows['sum_values'] = max_non_zero_rows[self.area_columns].sum(axis=1)
            max_row = max_non_zero_rows.loc[max_non_zero_rows['sum_values'].idxmax()]
        else:
            max_row = max_non_zero_rows.iloc[0]

        max_row = max_row.drop(['non_zero_count', 'sum_values'], errors='ignore')
        
        return pd.DataFrame([max_row])