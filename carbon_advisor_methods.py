from carbon_advisor_interface import CarbonAdvisorInterface

#carbon_scaler - change name - add init
#add to task class - marginal capacity
class CarbonAdvisor(CarbonAdvisorInterface):

    #get schedule of number of nodes to assign to each time slot
    def compute_schedule(self):
        # carbon_t = self.carbon_trace["carbon_intensity_avg"].values
        carbon_t = self.carbon_trace
        marginal_capacity = self.load_marginal_capacity()
        print("printing carbon t ", carbon_t)
        print("printing task profile ", self.task_profile)
        marginal_capacity_carbon = []
        # max_nodes = len(self.task_profile)  #maximum available nodes in the scale profile
        max_nodes = self.num_workers
        print("printing max nodes ", max_nodes)
        for i in range(0, len(self.carbon_trace)):
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

        #total samples that can be processed by 1 node given task length in seconds.
        total_samples = int(self.task_length * marginal_capacity[1]["throughput"] * 3600)
        
        task_schedule: dict[int, int] = {}

        done = 0
        while done < total_samples:
            for i in range(0, len(marginal_capacity_carbon)):
                print("printing done ", done)
                print("printing total samples ", total_samples)
                selection = marginal_capacity_carbon[i]
                done += self.task_profile[selection["nodes"]]   ["throughput"] * 3600   #increase the number of samples processed
                task_schedule[selection["timeslot"]]= selection["nodes"]    #assign number of nodes at timeslot
                del marginal_capacity_carbon[i]
                break
        print("task schedule h", task_schedule)
        self.task_schedule = task_schedule
        return task_schedule
    
    #load marginal power and throughput values to be used for task scheduling
    def load_marginal_capacity(self):
        marginal_model = {}
        model = self.task_profile
        marginal_model[1] = {"throughput": model[1]["throughput"],
                                "power": model[1]["power"]
                                }
        for i in range(2, 9):
            marginal_model[i] = {"throughput": model[i]["throughput"] - model[i - 1]["throughput"],
                                "power": model[i]["power"] - model[i - 1]["power"]}
        print("printing marginal model ", marginal_model)                        
        return marginal_model
    
    def get_scale_at_a_time(self, task_schedule):
        # Extract the number of worker nodes assigned at each time step. one time step = hour
        total_work = 0
        # total_samples = int(self.task_length * self.task_profile[1]["throughput"] * 3600)  # job unable to finish. 
        total_samples = int(self.task_length * self.task_profile[1]["throughput"])
    
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
        marginal_capacity = self.load_marginal_capacity()
        task_throughput = 0
        for timeslot, nodes in self.task_schedule.items():
            task_throughput += marginal_capacity[nodes]["throughput"]
        
        total_samples = int(self.task_length * marginal_capacity[1]["throughput"] * 3600)
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

scaler = CarbonAdvisor(deadline=50, slack=0, num_workers=8, task_length=24, location='AU-SA', task='resnet18', start_time= 0, start_date='2021-03-22', start_hour=8)
scaler.compute_schedule()
print("printing compute time", scaler.get_compute_time())
# scaler.get_total_emissions()

# ---------- Roshini 

# scaler1 = CarbonScalerAlgo(deadline=50, slack=8, num_workers=8, task_length=24, location='AU-SA', task='densenet121', start_date='2021-03-22', start_hour=8)
# result1 = scaler1.analyse_schedule()
# print(result1)
# scaler2 = CarbonScalerAlgo(deadline=50, slack=8, num_workers=8, task_length=24, location='SE-SE1', task='resnet18', start_date='2020-01-01', start_hour=12)
# scaler.carbon_scaler_algo()
# result2 = scaler2.analyse_schedule()


# print(result2)
# # print(results)