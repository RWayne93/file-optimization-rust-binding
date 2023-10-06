### target directory is excluded from this repo 

To get the rust code to compile you will need to make sure you have rust, pyo3, and maturin installed otherwise you wont be able
to call file_handler from python. 

to compile the binary for python you need to run maturin develop --release without the release flag you will compile a slower binary (dev profile)
