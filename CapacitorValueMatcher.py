# CapacitorValueMatcher.py
import pandas as pd
import numpy as np
import re
import os
import json
import threading
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class CapacitorValueMatcher:
    def __init__(self, input_file_path, output_dir="output", batch_size=10000, num_threads=4, checkpoint_interval=5000, progress_callback=None):
        self.input_file_path = input_file_path
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.num_threads = num_threads
        self.checkpoint_interval = checkpoint_interval
        self.progress_callback = progress_callback

        os.makedirs(output_dir, exist_ok=True)

        self.checkpoint_file = os.path.join(output_dir, "checkpoint.json")
        self.matched_temp_file = os.path.join(output_dir, "matched_temp.pkl")
        self.unmatched_temp_file = os.path.join(output_dir, "unmatched_temp.pkl")

        self.processed_rows = 0
        self.matched_results = []
        self.unmatched_results = []

        self.lock = threading.Lock()

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                self.processed_rows = checkpoint.get('processed_rows', 0)

                if os.path.exists(self.matched_temp_file):
                    with open(self.matched_temp_file, 'rb') as f:
                        self.matched_results = pickle.load(f)

                if os.path.exists(self.unmatched_temp_file):
                    with open(self.unmatched_temp_file, 'rb') as f:
                        self.unmatched_results = pickle.load(f)
                return True
            except:
                return False
        return False

    def save_checkpoint(self):
        checkpoint = {
            'processed_rows': self.processed_rows,
            'timestamp': datetime.now().isoformat(),
            'matched_count': len(self.matched_results),
            'unmatched_count': len(self.unmatched_results)
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        with open(self.matched_temp_file, 'wb') as f:
            pickle.dump(self.matched_results, f)
        with open(self.unmatched_temp_file, 'wb') as f:
            pickle.dump(self.unmatched_results, f)

    def extract_patterns(self, part_number):
        if pd.isna(part_number) or not isinstance(part_number, str):
            return []
        patterns = []
        for i in range(len(part_number) - 2):
            substring = part_number[i:i+3]
            if substring.isdigit():
                patterns.append(substring)
        for i in range(len(part_number) - 3):
            substring = part_number[i:i+4]
            if substring.isdigit():
                patterns.append(substring)
        patterns.extend(re.findall(r'\d*R\d*', part_number))
        patterns.extend(re.findall(r'R\d+', part_number))
        return list(set(patterns))

    def calculate_values(self, pattern):
        calculated_values = []
        if 'R' in pattern:
            if pattern.startswith('R'):
                numeric_part = pattern[1:]
                if numeric_part:
                    decimal_value = float('0.' + numeric_part)
                    calculated_values.append(decimal_value)
            else:
                parts = pattern.split('R')
                if len(parts) == 2:
                    before_r = parts[0] if parts[0] else '0'
                    after_r = parts[1] if parts[1] else '0'
                    decimal_value = float(before_r + '.' + after_r)
                    calculated_values.append(decimal_value)
        elif pattern.isdigit() and len(pattern) in [3, 4]:
            digits = pattern
            if len(digits) >= 2:
                first_digits = int(digits[:-1])
                last_digit = int(digits[-1])
                calculated_values.append(first_digits * (10 ** last_digit))
                if last_digit == 7:
                    calculated_values.append(first_digits * (10 ** -1))
                    calculated_values.append(first_digits * (10 ** -3))
                elif last_digit == 8:
                    calculated_values.append(first_digits * (10 ** -2))
                elif last_digit == 9:
                    calculated_values.append(first_digits * (10 ** -1))
                    calculated_values.append(first_digits * (10 ** -3))
                first_digit = int(digits[0])
                remaining_digits = int(digits[1:])
                calculated_values.append(remaining_digits * (10 ** first_digit))
        return calculated_values

    def convert_to_pf(self, value, unit):
        if pd.isna(value) or value == 0:
            return 0
        unit = str(unit).lower().strip()
        conversion_factors = {
            'pf': 1,
            'nf': 1000,
            'uf': 1000000,
            'µf': 1000000,
            'mf': 1000000000,
            'f': 1000000000000
        }
        return value * conversion_factors.get(unit, 1)

    def parse_value_column(self, value_str):
        if pd.isna(value_str):
            return 0, 'pf'
        value_str = str(value_str).strip()
        numeric_match = re.search(r'[\d.]+', value_str)
        if not numeric_match:
            return 0, 'pf'
        numeric_value = float(numeric_match.group())
        unit_match = re.search(r'[a-zA-Zµ]+', value_str)
        unit = unit_match_
