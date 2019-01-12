/*
* CS 6210 - Project 4
* Haoran Li
* GTid: 903377792
* Date: Nov.28, 2018
*/

#pragma once

#include <math.h>
#include <vector>

#include "mapreduce_spec.h"

using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::ifstream;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct fileOffset {
	string filename;
	int start_offset;	// specifies an offset within a file
	int end_offset;		// inclusive with start_offset, exclusive with end_offset
};

struct FileShard {
	int shard_id;
	vector<fileOffset*> pieces;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, vector<FileShard>& fileShards) {
	cout << "Start file shard procudure!" << endl;

	vector<string> filenames = mr_spec.input_files;

	// start shard file
	// calculate total size of the file and then to determine the shard way
	int totalsize;	
	FileShard* shard = new FileShard;
	int cur_bytes = 0, shard_id = 0;

	for(auto filename : filenames) {
		ifstream file(filename, ifstream::ate | ifstream::binary);
		fileOffset* file_shard = new fileOffset;
		
		file.seekg(0, file.end);
		int size = file.tellg();  // size of the file (KB)
		file.seekg(0, file.beg);

		// traverse the file to avoid one word being split into two files
		// which means to check the shard method more carefully
		int cur_offset = 0;
		char ch;

		while(!file.eof()) {
			file.seekg(cur_offset);

			int num_bytes_left = (mr_spec.map_kilobytes * 1000 - cur_bytes);
			int size_left = (size) - cur_offset;
			
			int num_to_seek = fmin(num_bytes_left, size_left);

			file.seekg(cur_offset + num_to_seek);
			file_shard->filename = filename;
			file_shard->start_offset = cur_offset;

			// in order to avoid the situation that one word being split into two files
			// we should find the next *newline* character starting from current seeking point 			
			if(num_to_seek != size_left) {
				char ch;
				int newline_offset = 0;
				while(ch != '\n') {
					file.get(ch);
					newline_offset++;
				}
				cur_offset += newline_offset;
			}

			file_shard->end_offset = file.tellg();
			
			//push the file_shard from back into the shard
			shard->pieces.push_back(file_shard);
			cur_offset += num_to_seek;
			file_shard = new fileOffset;
			cur_bytes += num_to_seek;

			if(cur_bytes >= mr_spec.map_kilobytes * 1000) { // attribute share_id
				shard -> shard_id = shard_id;
				fileShards.push_back(*shard);
				shard = new FileShard;
				shard_id++;
				cur_bytes = 0;
			}

			file >> ch;
		}

		totalsize += size;
	}

	shard->shard_id = shard_id;
	fileShards.push_back(*shard);

	auto shards_num = ceil(totalsize / (mr_spec.map_kilobytes * 1000));
	cout << "Total files size: " << totalsize << ", number of shards: " << shards_num << endl;
	cout << "Actual number of shards: " << fileShards.size() << endl;
	return true;
}
