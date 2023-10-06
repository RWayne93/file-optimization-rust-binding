use pyo3::prelude::*;
use std::collections::HashMap;
use chrono::NaiveDateTime;
use chrono::Timelike;
use std::fs::File;
use memmap::MmapOptions;
use rayon::prelude::*;
// use std::time::Instant;
use num_cpus;
use pyo3::types::IntoPyDict;

/// Formats the sum of two numbers as string.
// #[pyfunction]
// fn load_phone_calls_dict(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }


//type PhoneCallsDict = HashMap<String, HashMap<String, Vec<NaiveDateTime>>>;
type PhoneCallsDict = HashMap<String, HashMap<String, Vec<i64>>>;

fn process_lines(lines: Vec<String>) -> PhoneCallsDict {
    let mut local_phone_calls_dict = PhoneCallsDict::new();

    for line in lines {
        let parts: Vec<&str> = line.trim().split(": ").collect();
        let timestamp_str = parts[0];
        let phone_number = parts[1];

        let area_code = phone_number.split('(').nth(1).unwrap().chars().take(3).collect::<String>();
        let timestamp = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S").unwrap();

        if timestamp.hour() < 6 {
            local_phone_calls_dict
                .entry(area_code)
                .or_default()
                .entry(phone_number.to_string())
                .or_default()
                .push(timestamp.timestamp());
        }

        // if timestamp.hour() < 6 {
        //     local_phone_calls_dict
        //         .entry(area_code)
        //         .or_default()
        //         .entry(phone_number.to_string())
        //         .or_default()
        //         .push(timestamp);
        // }
    }
    local_phone_calls_dict
}

fn read_file(file_name: &str) -> Vec<String> {
    let file = File::open(file_name).expect("Failed to open file");
    let mmap = unsafe { MmapOptions::new().map(&file).expect("Failed to map file") };

    let content = std::str::from_utf8(&mmap).expect("Failed to convert to string");
    content.lines().map(String::from).collect()
}

#[pyfunction]
fn load_phone_calls_dict(data_dir: &str) -> PyResult<PyObject> {

    let paths: Vec<_> = std::fs::read_dir(data_dir).expect("Failed to read directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.file_name().unwrap().to_str().unwrap().starts_with("phone_calls") && path.extension().unwrap() == "txt")
        .collect();

    let all_lines: Vec<_> = paths.par_iter()
        .map(|path| read_file(path.to_str().unwrap()))
        .flatten()
        .collect();

    let chunk_size = all_lines.len() / num_cpus::get();
    let chunks: Vec<_> = all_lines.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect();

    let results: Vec<_> = chunks.into_par_iter().map(process_lines).collect();

    let mut phone_calls_dict = PhoneCallsDict::new();

    for local_dict in results {
        for (area_code, numbers) in local_dict {
            for (phone_number, timestamps) in numbers {
                phone_calls_dict
                    .entry(area_code.clone())
                    .or_default()
                    .entry(phone_number)
                    .or_default()
                    .extend(timestamps);
            }
        }
    }


    pyo3::Python::with_gil(|py| {
        let py_dict = phone_calls_dict
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    value.into_iter().map(|(key2, value2)| {
                        (
                            key2,
                            value2
                        )
                    }).collect::<HashMap<String, Vec<i64>>>(),
                )
            })
            .collect::<HashMap<String, HashMap<String, Vec<i64>>>>()
            .into_py_dict(py)
            .to_object(py);
        Ok(py_dict)
    })
    


    // pyo3::Python::with_gil(|py| {
    //     let py_dict = phone_calls_dict
    //         .into_iter()
    //         .map(|(key, value)| {
    //             (
    //                 key,
    //                 value.into_iter().map(|(key2, value2)| {
    //                     (
    //                         key2,
    //                         value2
    //                             .into_iter()
    //                             .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
    //                             .collect::<Vec<String>>(),
    //                     )
    //                 }).collect::<HashMap<String, Vec<String>>>(),
    //             )
    //         })
    //         .collect::<HashMap<String, HashMap<String, Vec<String>>>>()
    //         .into_py_dict(py)
    //         .to_object(py);
    //     Ok(py_dict)
    // })   
}


#[pymodule]
fn file_handler(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load_phone_calls_dict, m)?)?;
    Ok(())
}
