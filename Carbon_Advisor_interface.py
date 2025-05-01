from abc import ABC, abstractmethod
from carbon_intensity_service import CarbonIntensityService
from task_profile import TaskProfile
from datetime import datetime
# todo - description of inputs 
# add carbon intensity service as input
#necessary inputs - remove location since we are giving as carbon intensity service, startdatetime
#num_workers change to max_nodes 
#add a task class
class CarbonAdvisorInterface(ABC):
    """
    Args:
        deadline (int): Deadline in hours - describes number of hours from start time within which the task needs to be completed.
        task (string): Represents the task that needs to be scheduled.
        start_date (string): Represents the date that the task can be started at
        start_hour (int): Represents the number of hours into the start date that the task can be started at.
        task_length (int, optional): Approximate length of the task. Defaults to 500.
        slack (int, optional): Represents the the number of hours after deadline that can be used to complete the task. Defaults to 0.
        num_workers (int, optional): Number of nodes available to perform the task. Defaults to 8.
    """
    def __init__(
    self,
    carbon_intensity_service: CarbonIntensityService,
    task_profile_obj: TaskProfile,
    start_date_time: datetime,
    deadline: int,
    slack: int,
    max_nodes: int = 8
):
        self.start_date_time = start_date_time
        self.deadline = deadline
        self.slack = slack
        self.max_nodes = max_nodes

        #load carbon trace
        self.carbon_intensity_service = carbon_intensity_service
        
        self.task_profile_obj = task_profile_obj
        # self.carbon_trace = self.carbon_intensity_service.load_carbon_trace()

    @abstractmethod
    def compute_schedule(self):
        pass

    @abstractmethod
    def get_compute_time(self):
        pass

    @abstractmethod
    def get_total_emissions(self):
        pass
