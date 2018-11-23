#pragma once

#include <vector>
#include <fstream>
#include <math.h>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	std::vector<std::string> filenames;
	std::vector<int> from_offset;
	std::vector<int> to_offset;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	//TO DO:read file paths from mr_spec
	// TO DO: read map_kilobytes from mr_spec
	std::vector<string> inputs = mr_spec.input_files;
	int map_kilobytes = mr_spec.map_kilobytes;

	std::vector<int> file_sizes;
	std::vector<int> remaining_sizes; //this is needed why calculating the shards

	double size=0.0;
	for (int i = 0; i < inputs.size(); i++) {
		std::ifstream file(inputs[i], std::ios::in | std::ios::ate);
		if(!file.is_open()){
			std::cout << "Unable to open file\n";
		}
		size += (double)file.tellg()/1000; //size is in KB
		file_sizes.push_back(file.tellg()); //file_size is in bytes
		remaining_sizes.push_back(file.tellg());
		file.close();
	}

	int num_shards = ceil(size/map_kilobytes);

	int i=0, j=0;
	size = 0.0;
	int start_offset = 0;
	while (i < num_shards) {
		FileShard shard;
		while (j < inputs.size() && size + (double)remaining_sizes[j]/1000 <= map_kilobytes) {
			size += (double)remaining_sizes[j]/1000;

			shard.filenames.push_back(inputs[j]);
			shard.from_offset.push_back(start_offset);
			shard.to_offset.push_back(file_sizes[j]);
			j++;
			start_offset=0;
		}
		if(j<inputs.size()){
			int remaining = map_kilobytes - size;
			std::ifstream file(inputs[i], std::ios::in|std::ios::binary);
			if(!file.is_open()){
				cout << "Unable to open file\n";
			}
			int numBytes=0;
			file.seekg(remaining*1000, file.beg);
			char * c = (char *)malloc(1);
			while (!file.eof()) {
				file.read(c, 1);
				numBytes++;
				if(*c=='\n'){
					break;
				}
			}

			shard.filenames.push_back(inputs[j]);
			shard.from_offset.push_back(start_offset);
			shard.to_offset.push_back(remaining*1000+numBytes);
			size = 0.0;
			remaining_sizes[j] =remaining_sizes[j] - remaining*1000 - numBytes;
			fileShards.push_back(shard);
			i++;
			start_offset = remaining*1000+1+numBytes;
		} else {
			//push back the last shard
			fileShards.push_back(shard);
			break;
		}
	}

	for (int k=0; k<fileShards.size(); k++) {
		std::cout << "Shard " << k << std::endl;
		for (int l=0; l<fileShards[k].filenames.size(); l++) {
			std::cout << "File " << fileShards[k].filenames[l] << std::endl;
			std::cout << "File from offset " << fileShards[k].from_offset[l] << std::endl;
			std::cout << "File to offset " << fileShards[k].to_offset[l] << std::endl;
		}
	}
	return true;
}
