from carbon_advisor_interface import CarbonAdvisorInterface
from carbon_intensity_service import CarbonIntensityService
from task_profile import TaskProfile
import argparse
from datetime import datetime
from datetime import timedelta
import pandas as pd

#carbon_scaler - change name - add init
#add to task class - marginal capacity
class CarbonAdvisor(CarbonAdvisorInterface):
    def __init__(
        self,
        carbon_intensity_service: CarbonIntensityService,
        task_profile: TaskProfile,
        start_date_time: datetime,
        deadline: int,
        slack: int,
        max_nodes: int = 8
    ) -> None:
        super().__init__(carbon_intensity_service, task_profile, start_date_time, deadline, slack, max_nodes)

    
    def get_interval_carbon_intensity(self):
        carbon_t = self.carbon_intensity_service.carbon_trace
        start_date_time = self.start_date_time
        end_datetime = start_date_time + timedelta(hours=self.deadline + self.slack)

        carbon_t["datetime"] = pd.to_datetime(carbon_t["datetime"])

        #extract carbon intensity values at specified date and times
        carbon_t = carbon_t[
    (carbon_t["datetime"] >= start_date_time) &
    (carbon_t["datetime"] <= end_datetime)
]
        return carbon_t["carbon_intensity_avg"].values / 1000

    #get schedule of number of nodes to assign to each time slot
    def compute_schedule(self):
        # carbon_t = self.carbon_trace["carbon_intensity_avg"].values
        carbon_t = self.get_interval_carbon_intensity()
        marginal_capacity = self.task_profile_obj.load_marginal_capacity()
        print("printing carbon t ", carbon_t)
        print("printing task profile ", self.task_profile_obj)
        marginal_capacity_carbon = []
        # max_nodes = len(self.task_profile)  #maximum available nodes in the scale profile
        max_nodes = self.max_nodes
        print("printing max nodes ", max_nodes)
        for i in range(0, len(carbon_t)):
            for j in range(1, max_nodes + 1):
                value = marginal_capacity[j]["throughput"] / \
                    (marginal_capacity[j]["power"]
                    * carbon_t[i])  #get the value of throughput per unit carbon consumption for j nodes
                #append number of nodes j and and their throughput per unit carbon consumption
                marginal_capacity_carbon.append(
                    {
                        "timeslot": i,
                        "nodes": j,
                        "value": value
                    }
                )
        #sort in descending order of throughput per unit carbon consumption
        marginal_capacity_carbon = sorted(
        marginal_capacity_carbon, key=lambda x: x["value"], reverse=True)
        print("printing marginal capacity carbon ", marginal_capacity_carbon)

        total_samples = self.task_profile_obj.get_total_samples()
        
        task_schedule: dict[int, int] = {}

        done = 0
        while done < total_samples:
            for i in range(0, len(marginal_capacity_carbon)):
                print("printing done ", done)
                print("printing total samples ", total_samples)
                selection = marginal_capacity_carbon[i]
                done += self.task_profile_obj.task_profile[selection["nodes"]]   ["throughput"] * 3600   #increase the number of samples processed
                task_schedule[selection["timeslot"]]= selection["nodes"]    #assign number of nodes at timeslot
                del marginal_capacity_carbon[i]
                break
        print("task schedule h", task_schedule)
        self.task_schedule = task_schedule
        return task_schedule
    
    def get_scale_at_a_time(self, task_schedule):
        # Extract the number of worker nodes assigned at each time step. one time step = hour
        total_work = 0
        # total_samples = int(self.task_length * self.task_profile[1]["throughput"] * 3600)  # job unable to finish. 
        total_samples = int(self.task_profile_obj.task_length * self.task_profile[1]["throughput"])
    
        print(f"Required Total Work (total_samples): {total_samples}")

        for time, nodes in task_schedule.items():
            work_done = self.task_profile[nodes]["throughput"] * 3600
            total_work += work_done

            print(f"Time step: {time}, No. of workers: {nodes}, Work done: {work_done}, Total amount of work done: {total_work}")

            if total_work >= total_samples:
                print(f"Job completes at time step {time}")
                return time, {t: n for t, n in task_schedule.items()}
        
        print("Job did not complete within given task schedule.")
        return None, {t: n for t, n in task_schedule.items()} 

    def get_total_emission(self, task_schedule):
        
        # return sum(self.carbon_trace.iloc[t]["carbon_intensity_avg"] * self.task_profile[nodes]["power"] for t, nodes in task_schedule.items())
        emissions = []
        for t, nodes in task_schedule.items():
            emissions.append(self.carbon_trace.iloc[t]["carbon_intensity_avg"] * self.task_profile[nodes]["power"])
        print("testing if code is working")
        return sum(emissions)

    def get_total_energy(self, task_schedule):
        return sum(self.task_profile[nodes]["power"] for nodes in task_schedule.values())


    def get_compute_time(self):
        print("in get compute time")
        marginal_capacity = self.task_profile_obj.load_marginal_capacity()
        task_throughput = 0
        for timeslot, nodes in self.task_schedule.items():
            task_throughput += marginal_capacity[nodes]["throughput"]
        
        total_samples = int(self.task_profile_obj.task_length * marginal_capacity[1]["throughput"] * 3600)
        total_compute_time = total_samples/task_throughput
        return total_compute_time/3600
    
    #get the total carbon emissions on running the algorithm
    def get_total_emissions(self, type='carbon_aware'):
        total_emissions = 0
        task_schedule = {}

        #which schedule to compute emissions for
        if(type == 'carbon_agnostic'):
            task_schedule = self.carbon_agnostic_schedule
        else:
            task_schedule = self.task_schedule

        for t, nodes in task_schedule.items():

            #get carbon emissions at hour t
            carbon_value = self.carbon_trace["carbon_intensity_avg"].iloc[t]

            #power and throughput consumed by the nodes
            p = self.task_profile[nodes]["power"]

            emissions_t = p * carbon_value
            total_emissions += emissions_t
        return total_emissions

def main():
    parser = argparse.ArgumentParser(
            description="Carbon Advisor",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-t",
                        "--task",
                        type=str,
                        default="tinyimagenet-resnet18-weak",
                        help="Workload Yaml/CSV file")

    parser.add_argument("-l",
                        "--task_length",
                        type=float,
                        default=24,
                        help="Task Length")

    parser.add_argument("-c",
                        "--location",
                        type=str,
                        default="CA-ON",
                        help="Carbon Trace CSV")

    parser.add_argument("-d",
                        "--deadline",
                        type=int,
                        default=24,
                        help="Deadline Factor")
    parser.add_argument("-s",
                        "--slack",
                        type=int,
                        default=0,
                        help="Slack")
    parser.add_argument("-n",
                        "--num_workers",
                        type=int,
                        default=8,
                        help="Cluster Size")
    parser.add_argument("-sd",
                        "--start_date_time",
                        type=datetime.fromisoformat,
                        default=datetime.now(),
                        help="Start Hour")
    
    args = parser.parse_args()

    carbon_intensity_service = CarbonIntensityService(location=args.location)
    task_profile_obj = TaskProfile(task = args.task, task_length=args.task_length)
    task_profile_obj.load_scale_profile()
    scaler = CarbonAdvisor(
        carbon_intensity_service = carbon_intensity_service,
        task_profile = task_profile_obj,
        start_date_time = args.start_date_time,
        deadline=args.deadline,
        slack = args.slack,
        max_nodes=args.num_workers
    )

    scaler.compute_schedule()
if __name__ == "__main__":
    main()