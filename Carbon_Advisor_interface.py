from abc import ABC, abstractmethod
from carbon_intensity_service import CarbonIntensityService
# todo - description of inputs 
# add carbon intensity service as input
#necessary inputs - remove location since we are giving as carbon intensity service, startdatetime
#num_workers change to max_nodes 
#add a task class
class CarbonAdvisorInterface(ABC):
    def __init__(self, deadline, task, location, start_time, start_date, start_hour, task_length=500, slack=0, num_workers=8):
        self.deadline = deadline
        self.task = task
        self.location = location
        self.start_date = start_date
        self.start_hour = start_hour
        self.task_length = task_length
        self.slack = slack
        self.num_workers = num_workers
        self.start_time = start_time

        #load carbon trace
        self.carbon_intensity_service = CarbonIntensityService(location=self.location,
                                                              start_date=self.start_date,
                                                              start_time=self.start_time,
                                                              deadline=self.deadline,
                                                              task = self.task,
                                                              slack=self.slack)
        
        self.task_profile = self.carbon_intensity_service.load_scale_profile()
        self.carbon_trace = self.carbon_intensity_service.load_carbon_trace()

    @abstractmethod
    def compute_schedule(self):
        pass

    @abstractmethod
    def get_compute_time(self):
        pass

    @abstractmethod
    def get_total_emissions(self):
        pass
