#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <set>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {
	std::map<std::string, std::map<std::string, std::string>> fileToKeyValueMap;
	int num_reducers;
	std::string output_dir;
	int key_count;
	std::set<std::string> files;

	/* DON'T change this function's signature */
	BaseMapperInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	void flush();

	std::string getFilePath(std::string key);
	std::vector<std::string> getFilePaths();

};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
	key_count=0;
}

inline void BaseMapperInternal::flush(){
	//save any keys left in the map to file
	for (auto entry : fileToKeyValueMap) {
		std::ofstream file;
		file.open(entry.first, std::fstream::app);
		for (auto keyValue: entry.second) {
			file << keyValue.first <<" " << keyValue.second << "\n";
			file.flush();
		}
		file.close();
		files.insert(entry.first);
	}
	fileToKeyValueMap.clear();
	key_count=0;
}

//gets the intermediate file to store the data
inline std::string BaseMapperInternal::getFilePath(std::string key){
	return output_dir+"/intermediate" + std::to_string(std::hash<std::string>{}(key)%num_reducers) +".txt";
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	std::string intermediateFile = getFilePath(key);

	auto entry = fileToKeyValueMap.find(intermediateFile);
	if(entry!=fileToKeyValueMap.end()){
		auto keyEntry = entry->second.find(key);
		if(keyEntry!=entry->second.end()){
			int count = stoi(keyEntry->second);
			count+=stoi(val);
			entry->second[keyEntry->first] = std::to_string(count);
		} else {
			entry->second[key] = val;
			key_count++;
		}
	} else {
		std::map<std::string, std::string> tempMap;
		tempMap[key] = val;
		fileToKeyValueMap[intermediateFile] = tempMap;
		key_count++;
	}

	if(key_count >= 100){
		for (auto entry : fileToKeyValueMap) {
			std::ofstream file;
			file.open(entry.first, std::fstream::app);

			for (auto keyValue: entry.second) {

				file << keyValue.first <<" " << keyValue.second << "\n";
				file.flush();
			}
			file.close();
			files.insert(entry.first);
		}
		fileToKeyValueMap.clear();
		key_count=0;
	}
}

inline std::vector<std::string> BaseMapperInternal::getFilePaths() {
	std::vector<std::string> file_paths;
	for (auto entry : files) {
		file_paths.push_back(entry);
	}

	return file_paths;
}

/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

	std::string output_dir;
	std::string output_file;
	/* DON'T change this function's signature */
	BaseReducerInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	/* NOW you can add below, data members and member functions as per the need of your implementation*/

};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::fstream file;
	file.open(output_dir+"/"+output_file, std::fstream::in | std::fstream::out | std::fstream::app);
	file << key << " " << val << "\n";
	file.flush();
	file.close();
}
