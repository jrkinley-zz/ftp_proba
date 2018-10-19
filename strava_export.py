import os
import csv
import gzip
import zipfile
import pandas as pd

from collections import defaultdict
from fitparse import FitFile
from tqdm import tqdm


base_dir = 'data'
zip_dir = os.path.join(base_dir, 'strava')
pro_dir = os.path.join(base_dir, 'process')

metrics = [
    'avg_cadence', 
    'avg_heart_rate', 
    'avg_power', 
    'avg_speed', 
    'enhanced_avg_speed', 
    'enhanced_max_speed', 
    'intensity_factor', 
    'id',
    'max_cadence', 
    'max_heart_rate', 
    'max_power', 
    'max_speed', 
    'normalized_power', 
    'threshold_power', 
    'timestamp', 
    'total_ascent', 
    'total_calories', 
    'total_cycles', 
    'total_descent', 
    'total_distance', 
    'total_elapsed_time', 
    'total_fat_calories', 
    'total_timer_time', 
    'training_stress_score'
]


class StravaExport:    
    def __init__(self, path, out):
        self.zip_file = path
        self.out = out        
        self.id = int(os.path.basename(path).split('_')[1])
        self.rides = defaultdict(list)

    
    def __get_rides(self, activities):
        rides = defaultdict(list)
        with open(activities, 'r') as act:
            reader = csv.reader(act)
            next(reader) # skip header
            for row in reader:
                if row[3].lower() == 'ride':
                    rides[row[1]].append(row[-1])
        return rides


    def extract_zip(self):
        print('extracting "{}" to "{}"'.format(self.zip_file, self.out))
        with zipfile.ZipFile(self.zip_file, 'r') as z:
            z.extract('profile.csv', self.out)
            act_file = z.extract('activities.csv', self.out)
            act_ride = self.__get_rides(act_file)
            for files in act_ride.values():
                [z.extract(fit, self.out) for fit in files if fit.endswith('.fit.gz')]


    def athlete_pd(self):
        pro_csv = os.path.join(self.out, 'profile.csv')
        with open(pro_csv, 'r') as pro:
            reader = csv.reader(pro)
            head = next(reader)
            data = next(reader)
            athlete = pd.DataFrame([data], columns=head)
            athlete['id'] = self.id
            return athlete


    def __process_fit(self, fit_file):
        df = pd.DataFrame([])    
        if fit_file.endswith('.fit.gz'):
            with gzip.open(fit_file, 'rb') as fit:
                raw = fit.read()
                fitfile = FitFile(raw)
                try:
                    for session in fitfile.get_messages('session'):                   
                        head = list(session.get_values())
                        data = list(session.get_values().values())                    
                        df = df.append(pd.DataFrame([data], columns=head), sort=True)
                except ValueError:                    
                    pass
        return df


    def rides_pd(self):
        rides = pd.DataFrame([])
        act_path = os.path.join(self.out, 'activities')
        for fit_file in tqdm(os.listdir(act_path)):
            df = self.__process_fit(os.path.join(act_path, fit_file))
            rides = rides.append(df, sort=True)
        rides['id'] = self.id
        return rides

