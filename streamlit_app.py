import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from functions import No_MS2_Processor, MS2_DDA_Processor, MS2_Select_DataProcessor

def main():
    
    st.set_page_config(page_title="FastMS", layout="wide", initial_sidebar_state="collapsed")
    st.markdown(
        """
        <style>
        .reportview-container {
            background-color: #f0f0f0;
        }
        .sidebar .sidebar-content {
            background-color: #d3d3d3;
        }
        h1, h2, h3 {
            color: #ff6347;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
    """
    <h1 style="text-align: center; font-size: 60px">FastMS</h1>
    <h3 style="text-align: center; color: gray; font-size: 25px">A MS Data Processing Platform Created by Hailiang Li</h3>
    """,
    unsafe_allow_html=True
    )
    

    uploaded_file = st.file_uploader("Upload an Excel file", type="xlsx")
    
    if uploaded_file is not None:
        
        ## a_No_MS2
        original_data = pd.read_excel(uploaded_file)
        data_filled1 = original_data.fillna(0)
        data_sorted1 = data_filled1.sort_values(by=['Name', 'm/z', 'RT [min]'])

        area_columns1 = data_sorted1.columns[data_sorted1.columns.str.startswith('Area:')]
        processor1 = No_MS2_Processor(area_columns1)
        
        grouped_data1 = data_sorted1.groupby('Name', group_keys=False).apply(processor1.merge_rows).reset_index(drop=True)
        filtered_data1 = grouped_data1.groupby('Name', group_keys=False).apply(processor1.filter_rows).reset_index(drop=True)
        processed_data1 = filtered_data1.groupby('Name', group_keys=False).apply(processor1.process_group).reset_index(drop=True)
        
        processed_data1.replace(0, np.nan, inplace=True)

        ## b_MS2_DDA
        #original_data = pd.read_excel(file_path)
        data_filled2 = processed_data1.fillna(0)
        data_sorted2 = data_filled2.sort_values(by=['Name', 'm/z', 'RT [min]'])

        area_columns2 = data_sorted2.columns[data_sorted2.columns.str.startswith('Area:')]
        processor2 = MS2_DDA_Processor(area_columns2)

        grouped_data2 = data_sorted2.groupby('Name', group_keys=False).apply(processor2.merge_rows).reset_index(drop=True)
        filtered_data2 = grouped_data2.groupby('Name', group_keys=False).apply(processor2.filter_rows).reset_index(drop=True)
        processed_data2 = filtered_data2.groupby('Name', group_keys=False).apply(processor2.process_group).reset_index(drop=True)
        
        processed_data2.replace(0, np.nan, inplace=True)

        ## c_MS2_Selecting
        #data = pd.read_excel(file_path)
        data_filled3 = processed_data2.fillna(0)
        data_sorted3 = data_filled3.sort_values(by=['Name', 'm/z', 'RT [min]'])

        area_columns3 = data_sorted3.columns[data_sorted3.columns.str.startswith('Area:')]
        processor3 = MS2_Select_DataProcessor(area_columns3)
        
        processed_data3 = data_sorted3.groupby('Name', group_keys=False).apply(processor3.process_group).reset_index(drop=True)

        processed_data3.replace(0, np.nan, inplace=True)

        st.write("Data processed successfully!")
        
        # Download button for processed data
        # Write processed data to BytesIO object
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            processed_data3.to_excel(writer, index=False)
        output.seek(0)

        # Download button
        st.download_button(
            label="Download Processed Data",
            data=output,
            file_name="processed_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    df = pd.DataFrame(
        {
            "Name": ["L-Tyrosine"],
            "Formula": ["C9H11NO3"],
            "Calc.MW": [181.07409],
            "m/z": [182.08138],
            "RT[min]": [1.227],
            "MS2": ["DDA for preferred ion"],
            "Area: {XXX}": [1418262500]
        }
    )

    st.write('Dataframe Example')
    st.table(df)


if __name__ == "__main__":
    main()