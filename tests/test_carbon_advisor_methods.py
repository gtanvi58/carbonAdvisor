import unittest
from carbon_advisor_methods import CarbonAdvisor
from carbon_intensity_service import CarbonIntensityService
from task_profile import TaskProfile
from datetime import datetime, timezone
#todo - create fake carbon intensity service, fake task profiles, add tests to algorithm
#test for linear marginal capacity -schedules all tasks in 1 low carbon period. and when slack =0 and marginal capacity is 0, the algorithm should be similar to carbon agnostic
class test_carbon_advisor_methods(unittest.TestCase):
    def setUp(self):
        self.task_length = 24
        carbon_intensity_service = CarbonIntensityService(location='AU-SA')
        task_profile_obj = TaskProfile(task='resnet18', task_length=10)
        self.carbon_advisor = CarbonAdvisor(carbon_intensity_service = carbon_intensity_service,
        task_profile = task_profile_obj,
        start_date_time = datetime(2021, 3, 22, 8, 0, 0, tzinfo=timezone.utc),
        deadline=50,
        slack = 0,
        max_nodes=8)
        self.task_profile = task_profile_obj.load_scale_profile()

    def test_load_marginal_capacity(self):
        marginal_capacity = self.carbon_advisor.task_profile_obj.load_marginal_capacity()
        self.assertIsInstance(marginal_capacity, dict)
        for value in marginal_capacity.values():
            self.assertIsInstance(value, dict)
            self.assertIn('throughput', value)
            self.assertIn('power', value)
            self.assertIsInstance(value['throughput'], float)
            self.assertIsInstance(value['power'], float)
    
    def test_compute_schedule(self):
        marginal_capacity = self.carbon_advisor.task_profile_obj.load_marginal_capacity()
        task_schedule = self.carbon_advisor.compute_schedule()
        total_work = int(self.task_length * marginal_capacity[1]["throughput"] * 3600)
        self.assertIsInstance(task_schedule, dict)
        for key, value in task_schedule.items():
            self.assertIsInstance(key, int)
            self.assertIsInstance(value, int)
        
        done = 0
        for timeslot, nodes in task_schedule.items():
            done += self.carbon_advisor.task_profile_obj.task_profile[nodes]["throughput"] * 3600
        self.assertGreaterEqual(int(done), total_work)

    def test_get_compute_time(self):
        task_schedule = self.carbon_advisor.compute_schedule()
        compute_time = self.carbon_advisor.get_compute_time()
        self.assertIsInstance(compute_time, float)
    
if __name__ == '__main__':
    unittest.main()