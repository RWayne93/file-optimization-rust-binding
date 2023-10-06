import file_handler
from datetime import datetime
import time
from multiprocessing import Pool, cpu_count
import os

path = "/Users/ryan/Desktop/python_projects/ppp-input-output-files/data"

def format_timestamps(data_chunk):
    area_code, numbers = data_chunk
    for number, timestamps in numbers.items():
        formatted_timestamps = [datetime.fromtimestamp(ts) for ts in timestamps]  # Store as datetime objects
        numbers[number] = formatted_timestamps
    return area_code, numbers

def generate_phone_call_counts(phone_calls_dict):
    phone_call_counts = {}
    
    for _, numbers_dict in phone_calls_dict.items():  # Corrected this line
        for phone_number, calls in numbers_dict.items():
            phone_call_counts[phone_number] = len(calls)
            
    return phone_call_counts


def most_frequently_called(phone_call_counts, top_n):
    items = list(phone_call_counts.items())
    
    sorted_items = sorted(items, key=lambda x: (-x[1], x[0]))
    
    return sorted_items[:top_n]


def export_phone_call_counts(most_frequent_list, out_file_path):
    with open(out_file_path, 'w') as output_file:
        
        for phone_number, count in most_frequent_list:
            output_file.write(f"{phone_number}: {count}\n")

def export_redials_report(phone_calls_dict, report_dir):
    os.makedirs(report_dir, exist_ok=True)

    for area_code, ac_data in phone_calls_dict.items():
        report = []  

        for phone_number, call_data in sorted(ac_data.items()):
            sorted_timestamps = sorted(call_data)

            for i in range(len(sorted_timestamps) - 1):
                timestamp_1 = sorted_timestamps[i]
                timestamp_2 = sorted_timestamps[i + 1]

                time_delta = timestamp_2 - timestamp_1
                sec_diff = time_delta.total_seconds()

                if sec_diff < 600:
                    time_str_1 = timestamp_1.strftime("%Y-%m-%d %H:%M:%S")  
                    time_str_2 = timestamp_2.strftime("%H:%M:%S")
                    minutes, seconds = divmod(int(sec_diff), 60)
                    duration_str = f"{minutes:02}:{seconds:02}"
                    line = f"{phone_number}: {time_str_1} -> {time_str_2} ({duration_str})"
                    report.append(line)
        
        with open(os.path.join(report_dir, f"{area_code}.txt"), 'w') as file:
            if report:
                file.write('\n'.join(report)+'\n')

def main():
    start_time = time.time()
    data = file_handler.load_phone_calls_dict(path)

    data_chunks = list(data.items())

    with Pool(cpu_count()) as pool:
        results = pool.map(format_timestamps, data_chunks)

    formatted_data = dict(results)

    phone_call_counts = generate_phone_call_counts(formatted_data)
    most_frequent_list = most_frequently_called(phone_call_counts, 10)
    export_phone_call_counts(most_frequent_list, 'phone_call_counts.txt')
    export_redials_report(formatted_data, 'redials_report')

    end_time = time.time()
    duration = end_time - start_time
    print(f"Processing took {duration} seconds")

if __name__ == '__main__':
    main()

# start_time = time.time()
# data = file_handler.load_phone_calls_dict(path)
# end_time = time.time()
# duration = end_time - start_time
# print(f"Processing took {duration} seconds")