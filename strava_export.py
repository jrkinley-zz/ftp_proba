import os
import shutil
import csv
import gzip
import zipfile
import pandas as pd

from collections import Counter, defaultdict
from fitparse import FitFile
from fitparse.utils import FitCRCError
from tqdm import tqdm
from tabulate import tabulate as tab


base_dir = 'data'
zip_dir = os.path.join(base_dir, 'strava')
pro_dir = os.path.join(base_dir, 'process')

athlete_metrics = [
    'id',
    'email',
    'firstname',
    'lastname',
    'sex',    
    'weight'
]

ride_metrics = [
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
            next(reader)  # skip header
            for row in reader:
                if row[3].lower() == 'ride':
                    rides[row[1]].append(row[-1])
        return rides

    def extract_zip(self):
        print('Extracting "{}" to "{}"'.format(self.zip_file, self.out))
        with zipfile.ZipFile(self.zip_file, 'r') as z:
            z.extract('profile.csv', self.out)
            act_file = z.extract('activities.csv', self.out)
            act_ride = self.__get_rides(act_file)
            for files in act_ride.values():
                [z.extract(fit, self.out)
                 for fit in files if fit.endswith('.fit.gz')]

    def athlete_pd(self):
        pro_csv = os.path.join(self.out, 'profile.csv')
        with open(pro_csv, 'r') as pro:
            reader = csv.reader(pro)
            head = next(reader)
            data = next(reader)
            athlete = pd.DataFrame([data], columns=head)
            athlete['id'] = self.id
            return athlete.reindex(columns=athlete_metrics)

    def __process_fit(self, fit_file):
        df = pd.DataFrame([])
        if fit_file.endswith('.fit.gz'):
            with gzip.open(fit_file, 'rb') as fit:
                raw = fit.read()                
                fitfile = FitFile(raw)                
                for session in fitfile.get_messages('session'):
                    head = list(session.get_values())
                    data = list(session.get_values().values())                    
                    df = df.append(pd.DataFrame([data], columns=head), sort=True)
        df['id'] = self.id       
        return df.reindex(columns=ride_metrics)

    def rides_pd(self):
        rides = pd.DataFrame([])
        act_path = os.path.join(self.out, 'activities')
        c = Counter()
        for fit_file in tqdm(os.listdir(act_path), unit='file'):
            try:
                df = self.__process_fit(os.path.join(act_path, fit_file))
                rides = rides.append(df, sort=True)                
            except Exception as e:
                c[type(e).__name__] += 1
            else:
                c['success'] += 1
        print(c)
        return rides

def __mkpro():
    if os.path.exists(pro_dir) and os.path.isdir(pro_dir):
        print('Removing previous directory {}'.format(pro_dir))
        shutil.rmtree(pro_dir)

    print('Making directory {}'.format(pro_dir))
    os.makedirs(pro_dir)

def main():
    __mkpro()

    athletes = pd.DataFrame([])
    rides = pd.DataFrame([])

    zips = [f for f in os.listdir(zip_dir) if f.endswith('.zip')]
    for z in zips:
        print('Processing file: {}'.format(z))
        z_name = os.path.splitext(z)[0]
        z_path = os.path.join(zip_dir, z)
        z_out = os.path.join(pro_dir, z_name)

        x = StravaExport(z_path, z_out)
        x.extract_zip()
        df1 = x.athlete_pd()
        df2 = x.rides_pd()

        athletes = athletes.append(df1, sort=True)
        rides = rides.append(df2, sort=True)

    print(tab(athletes, tablefmt='psql'))
    print('Saving output to: {}'.format(pro_dir))
    
    athletes.to_csv(os.path.join(pro_dir, 'athletes.csv'), index=False)
    rides.to_csv(os.path.join(pro_dir, 'rides.csv'), index=False)

if __name__ == "__main__":
    main()
