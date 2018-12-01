#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <fstream>

using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int num_workers;
	int num_output_files;
	int map_kilobytes;
	string user_id;
	string output_dir;
	vector<string> input_files;
	vector<string> worker_ipaddr_ports;
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	fstream file;
	string line;
	file.open("../test/config.ini", fstream::in);
	size_t index;
	while(getline(file, line)) {
		if(line.find("n_workers=") != string::npos) {
			if (line.substr(10, int(line.size())) == "") {
				mr_spec.num_workers = 0;
			} else
				mr_spec.num_workers = stoi(line.substr(10, int(line.size())));
		}

		if(line.find("map_kilobytes=") != string::npos) {
			if (line.substr(14, int(line.size())) == "") {
				mr_spec.map_kilobytes = 0;
			} else
				mr_spec.map_kilobytes = stoi(line.substr(14, int(line.size())));
		}

		if(line.find("n_output_files=") != string::npos) {
			if (line.substr(15, int(line.size())) == "") {
				mr_spec.num_output_files = 0;
			} else
				mr_spec.num_output_files = stoi(line.substr(15, int(line.size())));
		}

		if(line.find("user_id=") != string::npos) {
			if (line.substr(8, int(line.size())) == "") {
				mr_spec.user_id = "";
			} else
				mr_spec.user_id = (line.substr(8, int(line.size())));
		}

		if(line.find("output_dir=") != string::npos) {
			if (line.substr(11, int(line.size())) == "") {
				mr_spec.output_dir = "";
			} else
				mr_spec.output_dir = (line.substr(11, int(line.size())));
		}

		if(line.find("input_files=") != string::npos) {
			int index;
			string input_files = line.substr(12, int(line.size()));
			int flag = 1;

			while(flag) {
				if(input_files.find(',') == string::npos) {
					flag = 0;
					// cout << input_files << "\n";
					mr_spec.input_files.push_back(input_files);
				} else {
					index = input_files.find(',');

					// cout << input_files.substr(0, index) << "\n";
					mr_spec.input_files.push_back(input_files.substr(0, index));
					input_files = input_files.substr(index+1, input_files.size());
					// cout << input_files << "\n";
				}
			}
		}

		if(line.find("worker_ipaddr_ports=") != string::npos) {
			int index;
			string ip_addr_port = line.substr(20, int(line.size()));
			int flag = 1;
			string ipaddr;

			while(flag) {
				if(ip_addr_port.find(',') == string::npos) {
					flag = 0;
					// cout << worker_ipaddr_ports.substr(input_files.find(':')+1, input_files.size()) << "\n";
					mr_spec.worker_ipaddr_ports.push_back(ip_addr_port);
				} else {
					index = ip_addr_port.find(',');
					ipaddr = ip_addr_port.substr(0, index);
					// cout << ipaddr.substr(ipaddr.find(':')+1, ipaddr.size()) << "\n";
					mr_spec.worker_ipaddr_ports.push_back(ipaddr);
					ip_addr_port = ip_addr_port.substr(index+1, ip_addr_port.size());
					// cout << input_files << "\n";
				}
			}
		}

	}

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	fstream file;
	bool ret_val = true;
	if(mr_spec.num_workers == 0 || mr_spec.map_kilobytes == 0 || mr_spec.num_output_files == 0) {
		std::cerr << "Specs not configured properly." << std::endl;
		ret_val = false;
	}

	if(mr_spec.user_id == "" || mr_spec.output_dir == "") {
		std::cerr << "Specs not configured properly." << std::endl;
		ret_val = false;
	}

	for(int i=0; i < mr_spec.input_files.size(); i++) {
		file.open(mr_spec.input_files[i], fstream::in);
		if(file.good() != 1) {
			std::cerr << "Specs not configured properly. Can't open input file" << std::endl;
			ret_val = false;
		}
		file.close();
	}

	if(mr_spec.worker_ipaddr_ports.size() < 1) {
		std::cerr << "Specs not configured properly" << std::endl;
		ret_val = false;
	}

	//https://stackoverflow.com/questions/12510874/how-can-i-check-if-a-directory-exists
	if(mr_spec.worker_ipaddr_ports.size() != mr_spec.num_workers) {
		std::cerr << "Number of workers not equal to number of worker ipaddresses" << std::endl;
		ret_val = false;
	}

	return ret_val;
}
