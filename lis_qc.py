""" QC for LIS data
# Author: Angel Acosta <angel.acosta@babsondx.com>
# Created 08/23/2022
"""

import pandas as pd
import numpy as np
import datetime 
from datetime import datetime
import os
import shutil
from prefect import flow,task


def is_float(a_string):
    try:
        float(a_string)
        return True
    except ValueError:
        return False

def str_is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def is_number(pd_series):
    type_series = []
    for i in pd_series.to_list():
        string_to_check = str(i).replace(" ", "")
        is_number = str_is_number(string_to_check)
        type_series.append(is_number)
    return pd.Series(type_series)

def test_result_convert_to_float(test_result_series):
    converted_series = []
    test_results = test_result_series.to_list()
    for result in test_results:
        new_result = str(result).strip(">").strip("<")
        if new_result.isnumeric():
            converted_series.append(float(new_result))
        elif is_float(new_result):
            converted_series.append(float(new_result))
        else:
            converted_series.append(np.nan)
    return pd.Series(converted_series)

@task
def validation_6(df):  #takes df, returns validation 6 series
    df["Validation 6"] = "" 
    condition1 = (df["Test Abbrev"].astype(str) == "~PLT-I") & (df["Instrument Flags"].isin(["W","A"]))
    sampleID_list = df.loc[condition1]["Sample ID"].unique()
    for sampleID in sampleID_list:
        sample_filter = (df["Sample ID"] == sampleID)
        record_ID = df.loc[sample_filter & condition1]["TestResultID"].to_list()[0]
        condition2 = np.any(np.where((df.loc[sample_filter]["Test Abbrev"] == "PLTCLUMP") & (df.loc[sample_filter]["Reported As"].isnull()==False), True,False))
        condition3 = np.any(np.where((df.loc[sample_filter]["Test Abbrev"] == "PLT_Clumps?") & (df.loc[sample_filter]["Reported As"].isnull()==False), True,False))
        condition4 = np.any(np.where((df.loc[sample_filter]["Test Abbrev"] == "PLT_Clumps? Confirmed?") & (df.loc[sample_filter]["Reported As"].isnull()==False), True,False))
        df.loc[sample_filter  & (df["TestResultID"] == record_ID),"Validation 6"]= np.where((condition2)&(condition3)&(condition4) ,"passed","Flag on a ~PLT-I result and missing corresponding Records")
    return df["Validation 6"]

@task
def validation_7(df): #takes df, returns validation 7 series
    df["Validation 7"] = "" 
    condition1 =  (df["Origin"].astype(str) == "Sysmex XN-1000") & (df["Instrument Flags"].astype(str) =="A") # we might be too strict when looking at origin across the board.
    sampleID_list = df.loc[condition1]["Sample ID"].unique() # this establishes which sampleIDs meet condition 1
    for sampleID in sampleID_list:
        sample_filter = (df["Sample ID"] == sampleID)
        subset_df = df.loc[sample_filter]
        result_ID_list = subset_df.loc[condition1]["TestResultID"].to_list()
        analyte_to_check_list = df.loc[(sample_filter) &(condition1)]["Test Abbrev"].astype(str).to_list() # should return a single result, NOT ALWAYS TRUE
        analyte_to_check_list = [ i+"?" if "?" not in i else i  for i in analyte_to_check_list ]
        record_dict = dict(zip(result_ID_list,analyte_to_check_list))
        for record_ID, analyte in record_dict.items():
            # Heath Said remove cond 2
            # condition2 = np.any(np.where((df.loc[sample_filter]["Test Abbrev"].str.contains(analyte,na=False)) & (df.loc[sample_filter]["Reported As"].isnull()==False)))
            analyte_confirmed = analyte + " Confirmed?" 
            condition3 = np.any(np.where((subset_df["Test Abbrev"] == analyte_confirmed) & (subset_df["Reported As"].astype(str).isin(["Yes","No"])), True,False))
            df.loc[sample_filter & (df["TestResultID"] == record_ID),"Validation 7"]= np.where((condition3) ,"passed","Hematology result is not flagged as confirmed")
    return df["Validation 7"]

@task  
def validation_9(df):
    df["Validation 9"] = ""
    condition1 = (df["Test Comments"].str.contains("Rerun",na=False)) & (df["Origin"].isin(val_9_origin_values))
    sampleID_list = df.loc[condition1]["Sample ID"].unique()
    for sampleID in sampleID_list:
        sample_filter = (df["Sample ID"] == sampleID)
        condition2 = (~df["Test Comments"].str.contains("Rerun",na=False))
        resultID_list = df.loc[sample_filter & condition1]["TestResultID"].to_list()
        aspiration_origin_list = df.loc[sample_filter & condition1][["AspirationTimestamp","Origin"]].values.tolist()
        record_dict = dict(zip(resultID_list,aspiration_origin_list))
        for resultID, timestamp_origin in record_dict.items():
            test_comment_bool = df.loc[df["TestResultID"] == resultID]["Test Comments"].isnull().to_list()[0]
            cond_3_df = df.loc[condition2 & (df["Origin"] == timestamp_origin[1]) & sample_filter]
            condition3 = np.any(np.where((timestamp_origin[0] < cond_3_df["AspirationTimestamp"]),True, np.where((timestamp_origin[0] == cond_3_df["AspirationTimestamp"]) & (test_comment_bool),True,False)))
            df.loc[sample_filter & (df["AspirationTimestamp"] == timestamp_origin[0]) & (df["TestResultID"] == resultID),"Validation 9"]= np.where((condition3) ,"Aspiration Time of repeat test is not after initial tests","passed")        
    return df["Validation 9"]

@task
def validation_16(df):
    df["Validation 16"] = ""
    for analyte, decimal_place in val_16_analyte_dict.items():
        analtye_filter = ((df["Test Abbrev"] == analyte)|(df["Test ASTM/HL7/SDF 1 Code"] == analyte)) & (is_number(df["Reported As"].astype(str)))
        filtered_df = df.loc[analtye_filter]
        if decimal_place != 0:
            df.loc[analtye_filter,"Validation 16"] = np.where(filtered_df["Reported As"].astype(str).str[::-1].str.find('.')==decimal_place,"passed","Test Result does not contain correct decimal places")
        else:
            df.loc[analtye_filter,"Validation 16"] = np.where(filtered_df["Reported As"].astype(str).str[::-1].str.find('.') == -1,"passed","Test Result does not contain correct decimal places")
    return df["Validation 16"]


def found_tests_dict(df,sampleID_list):
    sampleID_tests_dict = {}
    for sampleID in sampleID_list: #array of unique IDs
        group_filter = (df["Sample ID"]== sampleID) 
        found_tests = df.loc[group_filter]["Test Abbrev"].to_list()
        sampleID_tests_dict.update({sampleID: found_tests})
    return sampleID_tests_dict

@task
def validation_20(df):
    df["Validation 20"] = ""
    unique_sampleIDs = df["Sample ID"].unique()
    for sampleID in unique_sampleIDs:
        sample_filter = (df["Sample ID"] == sampleID)
        duplicate_filter = (df.loc[sample_filter]["Test Abbrev"].duplicated(keep=False) == True) & (df.loc[sample_filter]["Test ASTM/HL7/SDF 1 Code"].duplicated(keep=False) == True)
        df.loc[sample_filter ,"Validation 20"] =np.where(duplicate_filter,"Test occured more than once per Sample ID","passed") 
    return df["Validation 20"]

@task
def validation_25(df):
    df["Validation 25"] = "" 
    condition1 =  (df["Test Abbrev"].isin(ip_message_list)) & (df["Instrument Flags"].astype(str) =="A") # we might be too strict when looking at origin across the board.
    sampleID_list = df.loc[condition1]["Sample ID"].unique() # this establishes which sampleIDs meet condition 1
    for sampleID in sampleID_list:
        sample_filter = (df["Sample ID"] == sampleID)
        subset_df = df.loc[sample_filter]
        result_ID_list = subset_df.loc[condition1]["TestResultID"].to_list()
        analyte_to_check_list = df.loc[(sample_filter) &(condition1)]["Test Abbrev"].astype(str).to_list() # should return a single result, NOT ALWAYS TRUE
        analyte_to_check_list = [ i+"?" if "?" not in i else i  for i in analyte_to_check_list ]
        record_dict = dict(zip(result_ID_list,analyte_to_check_list))
        for record_ID, analyte in record_dict.items():
            analyte_confirmed = analyte + " Confirmed?" 
            condition3 = np.any(np.where((subset_df["Test Abbrev"] == analyte_confirmed) & (subset_df["Reported As"].astype(str).isin(["Yes","No"])), True,False))
            df.loc[sample_filter & (df["TestResultID"] == record_ID),"Validation 25"]= np.where((condition3) ,"passed","Ip Message Test result is not flagged as confirmed")
    return df["Validation 25"]

@task
def validation_28_and_29(df):
    df["Validation 28"] = ""
    df["Validation 29"] = ""
    unique_sampleIDs = df["Sample ID"].unique()
    for sampleID in unique_sampleIDs:
        sample_filter = (df["Sample ID"] == sampleID)
        filtered_df = df.loc[sample_filter]
        found_method_list = filtered_df["Test ASTM/HL7/SDF 1 Code"].to_list()
        check_cbc = any(item in found_method_list for item in list(cbc_dict.values()))
        check_chem = any(item in found_method_list for item in list(chem_dict.values()))
        
        if check_cbc & check_chem:
            print(sampleID,": found multiple panels")
        if check_cbc:
            cbc_bool = set(cbc_dict.values()).issubset(set(found_method_list))
            if cbc_bool:
                df.loc[sample_filter,"Validation 28"] ="passed"

                cbc_filter = (filtered_df["Test ASTM/HL7/SDF 1 Code"].isin(list(cbc_dict.values())))
                cbc_df= filtered_df.loc[cbc_filter]
                if all(~is_number(cbc_df['Reported As'])):
                    all_same_bool = (cbc_df['Reported As'] == cbc_df['Reported As'].iloc[0]).all()
                    if all_same_bool:
                        df.loc[sample_filter & cbc_filter , "Validation 29"] = "passed"
                    else:
                        df.loc[sample_filter & cbc_filter , "Validation 29"] = "Strings should be the same across panel"

            else:
                df.loc[sample_filter,"Validation 28"] ="Incomplete results in CBC panel"
                
        elif check_chem:
            chem_bool =set(chem_dict.values()).issubset(set(found_method_list))
            tsh_bool =set(["TSH3UL","X3PFH"]).issubset(set(found_method_list))
            if chem_bool or tsh_bool :
                df.loc[sample_filter,"Validation 28"] ="passed"
                chem_filter = (filtered_df["Test ASTM/HL7/SDF 1 Code"].isin(list(chem_dict.values())))
                chem_df= filtered_df.loc[chem_filter]
                if all(~is_number(chem_df['Reported As'])):
                    all_same_bool = (chem_df['Reported As'] == chem_df['Reported As'].iloc[0]).all()
                    if all_same_bool:
                        df.loc[sample_filter & chem_filter, "Validation 29"] = "passed"
                    else:
                        df.loc[sample_filter & chem_filter, "Validation 29"] = "Strings should be the same across panel"

            else:
                df.loc[sample_filter,"Validation 28"] ="Incomplete results in Chem panel"
    return df["Validation 28"],df["Validation 29"]


@task
def validation_33(df): 
    df["Validation 33"] = "" 
    condition1 = ( df["Test Abbrev"].str.contains("Confirmed?"))
    sampleID_list = df.loc[condition1]["Sample ID"].unique()
    for sampleID in sampleID_list:
        sample_filter = (df["Sample ID"] == sampleID)
        ip_message = df.loc[sample_filter & condition1]["Test Abbrev"].to_list()[0]
        ip_message_to_check = ip_message.split(" Confirmed?")[0]
        record_ID = df.loc[sample_filter & condition1]["TestResultID"].to_list()[0]
        condition2 = np.any(np.where((df.loc[sample_filter]["Test Abbrev"]) == ip_message_to_check , True,False))
        df.loc[sample_filter  & (df["TestResultID"] == record_ID),"Validation 33"]= np.where((condition2) ,"passed","missing corresponding Records")
    return df["Validation 33"]
    
@flow
def apply_validation_checks(df):
    df["Validation 6"] = validation_6(df)
    df["Validation 7"] = validation_7(df)
    test_result_float = test_result_convert_to_float(df["Reported As"])
    df["Validation 10"] = np.where((df["Test Abbrev"].astype(str) == "X3PFH") & (test_result_float.isnull() == False) & (test_result_float > 100) & (~df["Test Comments"].str.contains("Rerun",na=False)), "PFH > AMR - repeated without correct documentation","passed")
    df["Validation 16"] = validation_16(df)
    df["Validation 18"] = np.where((df["Sample ID"].str.split("-").str[-1] == df["Tube/Container Type"].astype(str)) | (df["Tube/Container Type"].astype(str) == "Slide"), "passed", "Wrong order choice for tube type")
    acceptable_string_list_22 = [i.lower() for i in acceptable_string_list]
    df["Validation 22"] = np.where((~is_number(df["Reported As"].astype(str))) & (df["Reported As"].str.lower().isin(acceptable_string_list_22) == False),"Unacceptable String Value in Reported As","passed")
    df["Validation 24"] = np.where(((df["Origin"] == "Sysmex XN-1000")) & (df["Instrument Flags"].isin(["W","A"])) & (~df["Test Comments"].str.contains("Rerun",na=False)),"Missing Repeat = 'Y'","passed")
    df["Validation 25"] = validation_25(df)
    df["Validation 26"] = np.where((df["Test Abbrev"].isin(ip_message_list) == False) & ((df["Test ASTM/HL7/SDF 1 Code"].isnull()) | (df["Test Clover Code"].isnull())),"Test ASTM/HL7/SDF 1 Code and TestClover Code should not be null","passed")
    df["Validation 28"],df["Validation 29"] = validation_28_and_29(df)
    df["Validation 33"]= validation_33(df)

 
@task
def split_val_df_noted_records(df):
    filter = (df["Test Comments"].str.startswith("NOTE:",na=False))
    noted_df = df.loc[filter]
    remaining_df = df.loc[~filter]

    return noted_df,remaining_df

@task
def write_report(output_path,worksheet_dict):
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheetname, sheet_df in worksheet_dict.items():
            try:           
                    sheet_df.to_excel(writer, sheet_name= sheetname, index = False, header=True) 
                    auto_adjust_col_width(writer,sheetname,sheet_df)                   
            except Exception as e:
                print(e)
        print("Output Has Been Written")

@flow
def generate_report(df,output_path): #takes dataframe, writes excel output. 
    validation_columns = [i for i in df.columns if "Validation" in i]
    condition1 = df[validation_columns].isin(["","passed"]).all(axis=1)
    good_data = df.loc[(condition1)]
    bad_data = df.loc[(condition1==False)]  
    all_data = df
    # check_for_data_loss(good_data, bad_data, all_data, original_data)
    summary_df = data_quality_summary(good_data, bad_data, all_data,validation_columns)
    summary_df = summary_df.rename(index = {0:'Value'}).transpose().reset_index()
    summary_df.rename(columns={"index": "Check"},inplace=True)

    noted_df,bad_df = split_val_df_noted_records(bad_data)
    worksheet_dict = {"Failed Validation":bad_df,"Commented and Failed":noted_df,"Passed Validation":good_data,"All Records":all_data, "Data Quality Summary":summary_df}
    write_report(output_path,worksheet_dict)

@task
def data_quality_summary(good_data, bad_data, all_data,validation_columns):
    summary_df= pd.DataFrame(index=range(1))
    good_count = len(good_data)
    bad_count = len(bad_data)
    all_count = len(all_data)
    summary_df["Total Passed Validation"] = good_count
    summary_df["Total Failed Validation"] = bad_count
    summary_df["Data Quality Score"] = round((good_count/all_count)*100, 3)
    for val in validation_columns:
        values_list = all_data[val].to_list()
        failed = [i for i in values_list if i not in ["","passed"]]
        summary_df["Failed " +val] = len(failed)
    return summary_df

def auto_adjust_col_width(writer,sheet,df):
    for column in df:
        column_width = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets[sheet].set_column(col_idx, col_idx, column_width)
            
def check_for_data_loss(good_data, bad_data, all_data, original_data):
    if ((len(bad_data)+len(good_data) != len(original_data)) |(len(original_data) != len(all_data))):
        print("Data loss occured, please check log file")

def read_excel_file(input):
    is_csv = input.endswith("csv")
    if is_csv:
        df = pd.read_csv(input, encoding="ANSI", keep_default_na=False, na_values="")
        df["TestResultID"] = df.apply(lambda x: hash(tuple(x)), axis = 1)
    else:
        df = pd.read_excel(input, keep_default_na=False, na_values="")
    return df

# our global variables for condition checking
val_1_analyte_list = ["Atypical_Lympho?","Left_Shift?","Fragments?"]
val_4_origin_values = ["Atellica1","Atellica2","BioRad D100"]
val_8_origin_values = ["Atellica1","Atellica2","BioRad D100","Sysmex XN-1000"]
val_9_origin_values = ["Atellica1","Atellica2","BioRad D100","Sysmex XN-1000"]
sysmex = "Sysmex XN-1000"
possible_nulls = ["", np.nan, "----", "NaN", None, np.datetime64('NaT'), "NaT"]
val_8_analyte_list = ["PLT_Abn_Distribution","Fragments?","Abn_Lympho?","ACTION_MESSAGE_Aged_Sample?","Atypical_Lympho?","Blasts?",
"Blasts/Abn_Lympho?","Dimorphic Population?","HGB_Defect?","Iron_Deficiency?","Left_Shift?",
"NRBC Present?","PLT_Clumps?","PRBC?","RBC_Agglutination?","Turbidity/HGB_Interference?"]
ip_message_list = ["WBC_Abn_Scattergram","RBC_Abn_Distribution","RBC_Agglutination?","PLT_Abn_Distribution", 
"PLT_Abn_Scattergram","PLT_Clumps?","Blasts?","Blasts/Abn_Lympho?","Atypical_Lympho?","Abn_Lympho?","Turbidity/HGB_Interference?",
"Left_Shift?","NRBC_Present","Iron_Deficiency?","Fragments?"] 

val_16_analyte_dict = {'WBC':2, 'RBC':2,'HGB':1 ,'HCT':1,'MCV':1,'MCH':1,'MCHC':1,'RDW-SD':1,'RDW-CV':1,'~PLT-I':0,'Neut%':1,
'Lymph%':1,'MONO%':1,'EO%':1,'BASO%':1,'ALB':2,'ALKP':0,'ALT':0,'AST':0,'BUN':0,'CA_2':1,'CHOL':0,'CL':0,'CO2':1,'CREA':2,
'GluH_3':0,'HDL':1,'K':2,'LDL':1,'NA':0,'TBIL':2,'TP':1,'TRIG':0,'TSH':3,'VITD':2,'X3PFH':3}

acceptable_string_list = ["No sample provided to Test", "Collection not attempted","No sample received","Unable to collect",
"Quantity not sufficient for repeat analysis","Quantity not sufficient for testing","Specimen hemolyzed",
"Test not performed due to special occurrence","This would include if the sample was dropped",
"Test not Performed (Above fill line)","Error","Yes","No","Slight","Moderate","Marked","None Seen"]

acceptable_string_list__23 = ["A","No sample provided to Test", "Collection not attempted","No sample received","Unable to collect",
"Quantity not sufficient for repeat analysis","Specimen hemolyzed","Test not performed due to special occurrence",
"This would include if the sample was dropped","Test not Performed (Above fill line): When the tube was overfilled",
"Error","Yes","No","Slight","Moderate","Marked","None Seen"]

cbc_dict = {
    "BA%":"BASO%",
    "EO%":"EOS%",
    "HCT":"HCT",
    "HGB":"HGB",
    "LY%":"LYMPH%",
    "MCH":"MCH",
    "MCHC":"MCHC",
    "MCV":"MCV",
    "MO%":"MONO%",
    "NE%":"NEUT%",
    "~PLT-I":"PLT-I",
    "RBC":"RBC",
    "RDW":"RDW-SD",
    "RDW":"RDW",
    "WBC":"WBC"
}

chem_dict = {
    "ALKP":"ALP_2C",
    "CA_2":"CA_2",
    "VITD":"VitD",
    "CO2":"CO2_c",
    "BUN":"UN_c",
    "CL":"CL",
    "CREA":"ECre_2",
    "HDL":"D_HDL",
    "K":"K",
    "TBIL":"Tbil_2",
    "NA":"Na",
    "CHOL":"Chol_2",
    "ALB":"AlbP",
    "AST":"AST",
    "ALT":"ALT",
    "TRIG":"TRIG",
    "GluH_3":"GluH_3",
    "LDL":"DLDL",
    "X3PFH":"X3PFH",
    "TP":"TP"
}

lis_lab_map = {
"Patient ID":	"SubjectID",
"Sample ID":	"SampleID",
"Test Abbrev":	"Analyte",
"Test ASTM/HL7/SDF 1 Code":	"Method",
"Units":	"TestResultUnits",
"Reported As":	"TestResultValue",
"Instrument Flags":	"TestResultFlags",
"Test Clover Code":	"TestCloverCode",
"Test Comments":	"Test Comments",
"Origin":	"Origin",
"Tube/Container Type":	"Tube",
"Text Result":	"ResultStr",
"NumRes":	"ResultNum"
}

@flow
def process_lis_data(input_file_path,output_directory):
    input_file_name = os.path.basename(input_file_path)
    output_string = input_file_name.split(".")[0]
    date = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
    output_file = f"\Validation Results for {output_string} {date}.xlsx"
    output_path = output_directory + output_file
    df = read_excel_file(input_file_path)
    original_data = df.copy()
    apply_validation_checks(df)
    generate_report(df,output_path)


def process_all(dir):
    input_directory = dir+"/input"
    output_directory= dir+"/output"
    fail_directory = dir+"/fail"
    success_directory = dir+"/success"

    for filename in os.scandir(input_directory):
        if filename.is_file():
            input_file_path =filename.path
            input_file_name = os.path.basename(input_file_path)
            try:
                process_lis_data(input_file_path,output_directory)
            except Exception as e:
                #if processing data fails, we want to fail the flow and send file to fail folder
                print(e)
                print(f"processing failed for {input_file_name}, sending to failed folder")
                shutil.move(input_file_path,(fail_directory +"/"+input_file_name))
                
            #if it gets here then it was successfull and we can move it to success folder 
            shutil.move(input_file_path,(success_directory +"/"+input_file_name))

if __name__ == '__main__':   

    ########## INPUT HERE ###########
    #dir should be the only thing needing to change on the server.

    dir = r"/lis_qc/lis_data"
    process_all(dir)
    ########## OUTPUT HERE ###########

















