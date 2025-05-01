import yaml

class TaskProfile:
    def __init__(self, task: int, task_length: int):
        self.task = task
        self.task_length = task_length

    #load scale profile for specified task
    def load_scale_profile(self):
        with open('./scale_profile.yaml', 'r') as f:
            scale_profile = yaml.safe_load(f)

        tp = scale_profile[self.task]
        print("printing tp replicas ", tp["replicas"])
        self.task_profile = tp["replicas"]

    
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
        self.marginal_capacity = marginal_model                        
        return marginal_model
    
    #total samples that can be processed by 1 node given task length in seconds.
    def get_total_samples(self):
        return int(self.task_length * self.marginal_capacity[1]["throughput"] * 3600)