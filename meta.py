import os
import json

import requests
from tqdm import tqdm
from ratelimit import limits, sleep_and_retry


class ExtenderCore:
    @staticmethod
    def get_progress(filename):
        if not os.path.exists(filename):
            return set()
        else:
            with open(filename) as f:
                finished = set([line.strip().split(",")[0] for line in f.readlines()])
            return finished
    
    @staticmethod
    @sleep_and_retry
    @limits(calls=10, period=1)
    def call_altm_api(url):
        r = requests.get(url)
        r.raise_for_status()
        return r.json()


class AltmExtender(ExtenderCore):

    def __init__(self, meta_path, meta_extraction, out_file, progress_file, id_type="doi"):
        self.meta_path = meta_path
        self.paper_meta = meta_extraction(meta_path)
        self.out_file = out_file
        self.progress_file = progress_file
        self.id_type = id_type

    def get_altm_meta(self):
        finished = self.get_progress(self.progress_file)
        with open(self.out_file, "a") as out_f, open(self.progress_file, "a") as finished_f:
            for id in tqdm(self.paper_meta):
                if id in finished:
                    continue
                try:
                    result = self.call_altm_api(f"https://api.altmetric.com/v1/{self.id_type}/{id}")
                    result["meta_id"] = id
                except Exception as e:
                    finished_f.write(f"{id},{e}\n")
                else:
                    out_f.write(json.dumps(result)+"\n")
                    finished_f.write(f"{id},\n")


if __name__ == "__main__":

    def get_plos_id(filepath):
        id_list = []
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if "correction" not in line.split(",")[0]:
                    id_list.append(line.split(",")[1])
        return id_list

    extender = AltmExtender("plosone_filelist.csv", get_plos_id, "plos_altm.json", "plos_altm_progress.txt")
    extender.get_altm_meta()
