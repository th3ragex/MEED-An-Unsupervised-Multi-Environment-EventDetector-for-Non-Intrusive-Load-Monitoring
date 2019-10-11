import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
from typing import Tuple, Dict, Any, Union

def load_file(file_path:Union[str, Path], phase:str="b")->Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Function to load the BLUED test data.
     Args:
            file_path (Path): full path to the test file
            phase (string): either "all", "b" or "a". Returns only the requested phase of the dataset.

    Returns:
            data_df (pandas DataFrame): original columns if phase=="all" else colums are just "Current" and "Voltage" --> already for the matching phase! (* - 1 done for B)
            file_info (dict): dictionary with information about the file that was loaded. Parsed from the filename
            and the metadata included in the file.
    """
    with open(file_path, 'r') as f:

        data_txt = f.read()

        #lines = data_txt.splitlines()

        data_txt = data_txt.split("***End_of_Header***")
        reference_time = data_txt[0].split("Date,")[1][:11].replace("\n","") +"-"+ data_txt[0].split("Time,")[1][:15]
        reference_time = datetime.strptime(reference_time, '%Y/%m/%d-%H:%M:%S.%f')


        data_time_str = data_txt[1].split("Time,")[1]
        data_time_str = data_time_str.split(',')

        data_day_str = data_txt[1].split("Date,")[1]
        data_day_str = data_day_str.split(',')

        day_str = data_day_str[0]  # just the first on is enoguh
        time_str = data_time_str[0][:15]  # same for time
        date = day_str + "-" + time_str
        start_date_time = datetime.strptime(date, '%Y/%m/%d-%H:%M:%S.%f')

        filename = Path(file_path).name  # get the file name

        samples = data_txt[1].split("Samples,")[1].split(",")[0:3][0]
        samples = int(samples)

        values_str = data_txt[-1]
        values_str = values_str[values_str.index("X_Value"):]

        measurement_steps = data_txt[1].split("Delta_X")[1].split(",")[0:3]
        measurement_steps = [float(x) for x in measurement_steps if x != ""]
        measurement_steps = measurement_steps[0]

        data_df = pd.read_csv(StringIO(values_str), usecols=["X_Value", "Current A", "Current B", "VoltageA"])

        data_df.dropna(inplace=True,how="any")
        
        file_duration = data_df.tail(1)["X_Value"].values[0]
        file_duration = float(file_duration)

        file_duration = timedelta(seconds=file_duration)
        end_date_time = reference_time + file_duration

        file_duration = end_date_time - start_date_time

        # Convert totimestamps
        data_df["TimeStamp"] = data_df["X_Value"].apply(lambda x: timedelta(seconds=x) + reference_time)
        data_df.drop(columns=["X_Value"],inplace=True)
        data_df.set_index("TimeStamp",inplace=True)

        file_info = {"Filepath": file_path, "Filename": filename, "samples": samples,
                        "file_start": start_date_time, "file_duration": file_duration, "file_end": end_date_time,
                        "measurement_steps": measurement_steps,"reference_time":reference_time}

        if phase.lower() != "all":
            if phase.lower() == "a":
                data_df["Current"] = data_df["Current A"]
                data_df["Voltage"] = data_df["VoltageA"]
            elif phase.lower() == "b":
                data_df["Current"] = data_df["Current B"]
                data_df["Voltage"] = data_df["VoltageA"].values * -1
            else:
                raise ValueError("The phase provided does not exist")

            data_df.drop(columns=['Current A', 'Current B',"VoltageA"],inplace=True)

    return data_df, file_info


def load_labels(file_path:Union[str, Path], file_start:datetime, file_end:datetime, phase:str='b'):
    labels_df = pd.read_csv(file_path, usecols=["Timestamp", "Label", "Phase"])

    #filter phase
    if phase.lower() != "all":
        labels_df = labels_df[labels_df.Phase == phase.upper()]

    #str -> datetime
    labels_df.Timestamp = pd.to_datetime(labels_df.Timestamp, infer_datetime_format=True)
    
    #filter timerange
    time_range_condition = (labels_df.Timestamp >= file_start) & (labels_df.Timestamp <= file_end)
    labels_df = labels_df[time_range_condition]
    labels_df.set_index("Timestamp", inplace=True)

    return labels_df