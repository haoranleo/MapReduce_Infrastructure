/*
* CS 6210 - Project 4
* Haoran Li
* GTid: 903377792
* Date: Nov.28, 2018
*/

#pragma once

#include <iostream>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <iterator>
#include <algorithm>
#include <sys/stat.h>

using std::cout;
using std::endl;
using std::copy;
using std::string;
using std::vector;
using std::ifstream;
using std::stringstream;
using std::back_inserter;
using std::istream_iterator;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	vector<string> worker_ipaddr_ports;				// workers ip addresses
	vector<string> input_files;						// input files
	string config_filename;							// config file's path
	string user_id;									// user id
	string output_dir;								// output file path
	int num_workers;								// number of workers
	int num_output_files;							// number of to be created output files
	int map_kilobytes;								// how much KB in each shard
};

inline void split(const string &str, char sep_ch, vector<string> &elem) {
    stringstream ss;
    string temp;
    ss.str(str);
    while (std::getline(ss, temp, sep_ch)) {
        elem.push_back(temp);
    }
}

inline vector<string> split(const string &str, char sep_ch) {
    vector<string> elem;
    split(str, sep_ch, elem);
    return elem;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const string& config_filename, MapReduceSpec& mr_spec) {
	ifstream config_file(config_filename);

	if(config_file.good()) {
		mr_spec.config_filename = config_filename;

		vector<string> lines;
		copy(istream_iterator<string>(config_file), istream_iterator<string>(), back_inserter(lines));

		for(auto line : lines) {
			vector<string> key_value = split(line, '=');
			string key = key_value.at(0), value = key_value.at(1);

			// read content in config file
			if(key.compare("n_workers") == 0) {
				mr_spec.num_workers = atoi(value.c_str());
			}
			if(key.compare("worker_ipaddr_ports") == 0) {
				mr_spec.worker_ipaddr_ports = split(value, ',');
			}
			if(key.compare("input_files") == 0) {
				mr_spec.input_files = split(value, ',');
			}
			if(key.compare("output_dir") == 0) {
				mr_spec.output_dir = value;
			}
			if(key.compare("n_output_files") == 0) {
				mr_spec.num_output_files = atoi(value.c_str());
			}
			if(key.compare("map_kilobytes") == 0) {
				mr_spec.map_kilobytes = atoi(value.c_str());
			}
			if(key.compare("user_id") == 0) {
				mr_spec.user_id = value;
			}
		}
		return true;
	} else {
		cout << "Failure in opening config file: " << config_filename << endl;
		return false;
	}
	return false;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	cout << "Name of config file: " << mr_spec.config_filename << endl;
	cout << "Number of workers: " << mr_spec.num_workers << endl;

	cout << "Worker IP addresses: " << endl;
	for(auto ip : mr_spec.worker_ipaddr_ports) {
		cout << "\t" << ip << endl;
	}

	cout << "Input file list: " << endl;
	for(auto in_file : mr_spec.input_files) {
		cout << "\t" << in_file << endl;
	}

	cout << "Output directory: " << mr_spec.output_dir << endl;
	cout << "Number of output files: " << mr_spec.num_output_files << endl;
	cout << "Number of kilobytes in each shard: " << mr_spec.map_kilobytes << endl;
	cout << "User ID: " << mr_spec.user_id << endl;

	// check the number of workers is consistent with number of given ip addresses
	if(mr_spec.num_workers != mr_spec.worker_ipaddr_ports.size()) {
		cout << "Number of workers doesn't match to the number of given ip addresses" << endl;
		cout << "Program terminate!" << endl;
		return false;
	}

	// check input file
	for(auto input_file : mr_spec.input_files) {
		ifstream input_file_s(input_file.c_str());
		if(!input_file_s.good()){
			cout << "Input file " << input_file << " doesn't exist!" << endl;
			cout << "Program terminate!" << endl;
			return false;
		}
	}

	return true;
}
