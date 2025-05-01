import pandas as pd
import datetime
import yaml
import os

#todo - remove unnecessary inputs - give current carbon intensity service and predictions, no need to filter trace in this code
#getCIatTime(), def predict(), loading in init, add types to inputs
# add a task class - with length
class CarbonIntensityService:
    def __init__(self, location: str):
        self.location = location
        self.csv_path = f"traces/{self.location}.csv"

        #load carbon trace
        csv_path = f"traces/{self.location}.csv"
        carbon_t = pd.read_csv(csv_path)

        #extract carbon intensity values and their date and time values
        self.carbon_trace = carbon_t[["datetime", "carbon_intensity_avg"]]

    def load_carbon_trace(self):
        csv_path = f"traces/{self.location}.csv"
        carbon_t = pd.read_csv(csv_path)

        #extract carbon intensity values and their date and time values
        carbon_t = carbon_t[["datetime", "carbon_intensity_avg"]]
        
        start_datetime = datetime.datetime.strptime(self.start_date, "%Y-%m-%d") + datetime.timedelta(hours=self.start_time)
        end_datetime = start_datetime + datetime.timedelta(hours=self.deadline + self.slack)

        carbon_t["datetime"] = pd.to_datetime(carbon_t["datetime"]).dt.tz_localize(None)

        #extract carbon intensity values at specified date and times
        carbon_t = carbon_t[
    (carbon_t["datetime"] >= start_datetime) &
    (carbon_t["datetime"] <= end_datetime)
]
        print("printing carbon ", carbon_t[5:])
        carbon_t["carbon_intensity_avg"] /= 1000
        return carbon_t["carbon_intensity_avg"].values

    
    #load scale profile for specified task
    def load_scale_profile(self):
        with open('./scale_profile.yaml', 'r') as f:
            scale_profile = yaml.safe_load(f)

        tp = scale_profile[self.task]
        print("printing tp replicas ", tp["replicas"])
        return tp["replicas"]

    def get_carbon_intensity_values(self):
        """Returns the filtered carbon intensity trace."""
        return self.carbon_trace["carbon_intensity_avg"].values / 1000  # Convert to kW

    def get_carbon_intensity_at_time(self, time_slot):
        """Returns the carbon intensity for a specific time slot."""
        return self.carbon_t["carbon_intensity_avg"].iloc[time_slot] / 1000  # Convert to kW
