# Data Files Convertor

[![MIT License](https://img.shields.io/badge/GitHub-100000)](https://github.com/povstenko/parquet_convertor)
[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://github.com/tterb/atomic-design-ui/blob/master/LICENSEs)

This script allows the user to convert different data types. 

This tool accepts comma-separated value files (.csv) as well as apache parquet (.parquet) files. It is assumed that the first row of the spreadsheet is the location of the columns.

This script requires that `pandas`, `argparse`, `pyarrow`, `csv` and `json` be installed within the Python environment you are running this script in.

This file can also be imported as a module and contains the following functions:

* `csv_to_parquet` - convert csv to parquet and save to file
* `parquet_to_csv` - convert parquet to csv and save to file
* `csv_to_json` - convert csv to json and save to file
* `parquet_schema` - returns schema of parquet file
* `add_filename_suffix` - returns filename string with added suffix for filename and change extension
* `is_file_ext_correct` - returns returns True if filename has correct file extension and prints message otherwise
* `main` - the main function of the script

## Table of Contents
- [Parameters](#parameters)
- [Usage](#usage)
  * [Convert CSV to Parquet](#convert-csv-to-parquet)
  * [Convert Parquet to CSV](#convert-parquet-to-csv)
  * [Convert CSV to JSON](#convert-csv-to-json)
  * [Get Parquet schema](#get-parquet-schema)
- [License](#license)

## Parameters
* `-h`, `--help` show help message and exit
* `-cp`, `--csv2parquet` convert csv to parquet. Set input csv filename string *(example: data.csv)*
* `-pc`, `--parquet2csv` convert parquet to csv. Set input parquet filename string *(example: data.parquet)*
* `-cj`, `--csv2json` convert csv to json. Set input csv filename string *(example: data.csv)*
* `-s`, `--get_schema` get schema of parquet file. Set input parquet filename string *(example: data.parquet)*
* `-o`, `--output` set output file name without extension *(example: newfile)*
* `-d`, `--delimiter` set delimiter for csv file *(default: ,)*
* `-i`, `--json_indent` set indent for json file *(default: None)*

## Usage

### Convert CSV to Parquet

Assume that you have some data in your `data.csv` file:
```
id,first_name,second_name,age
0,Vitaliy,Povstenko,19
1,John,Doe,25
2,Bill,Gates,40
3,Elon,Musk,30
4,Don,Joel,25
```
 need to convert some `data.csv` file to `parquet`, so you need to write the following command:
```
$python convertor.py --csv2parquet data.csv
```
And you receive new file `data_converted.parquet` in your directory

### Convert Parquet to CSV
You can specify `--parquet2csv` parameter in order to convert `data_converted.parquet` file back to csv
```
$python convertor.py --parquet2csv data_converted.parquet
```
New `data_converted_converted.parquet` now added to your directory, but it is good to specify the output file name *(without extension)* in parameter `--output`:
```
$python convertor.py --parquet2csv data_converted.parquet --output newfile
```
Successfully converted from `data_converted.parquet` to `newfile.csv`.
In order, you need to save or read CSV files using a special delimiter:
```
$python convertor.py -pc data_converted.parquet -o newfile --delimiter ;
```
Now file `newfile.csv` id delimited by `;`:
```
id;first_name;second_name;age
0;Vitaliy;Povstenko;19
1;John;Doe;25
2;Bill;Gates;40
3;Elon;Musk;30
4;Don;Joel;25
```

### Convert CSV to JSON
The script allows you to convert from CSV to JSON in a similar way:
```
$python convertor.py --csv2json data.csv  
```
`data_converted.json` looks like:
```
[{"id": "0", "first_name": "Vitaliy", "second_name": "Povstenko", "age": "19"}...]
```
In default, script save JSON file without the indent, but you can specify indent in new JSON file:
```
$python convertor.py -cj data.csv -o names --json_indent 4
```
New `names.json` contains:
```
[
    {
        "id": "0",
        "first_name": "Vitaliy",
        "second_name": "Povstenko",
        "age": "19"
    }
...
]
```

### Get Parquet schema
There are some cases when you need to know the schema of your Parquet file. For example:
```
$python convertor.py --get_schema data_converted.parquet
```
The script produces the following output:
```
id: int64
first_name: string
second_name: string
age: int64
```


## License
Apache 2.0 License: [www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0/) or see [the `LICENSE` file](https://github.com/povstenko/parquet_convertor/blob/main/LICENSE).
