/*
* CS 6210 - Project 4
* Haoran Li
* GTid: 903377792
* Date: Dec.2, 2018
*/

#pragma once

#include <map>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <utility>


using std::map;
using std::hash;
using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::ifstream;
using std::ofstream;
using std::istringstream;
using std::out_of_range;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

    /* DON'T change this function's signature */
    BaseMapperInternal();

    /* DON'T change this function's signature */
    void emit(const string& key, const string& value);

    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    void write_data(string name, int num_output_files);
    // map is already sorted
    vector<string> file_names;
    map<string, vector<string>> pairs;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const string& key, const string& value) {
    try{
        pairs.at(key).push_back(value);
    } catch(out_of_range const& e){
        vector<string> vec {value};
        pairs.emplace(key, vec);
    }
}

inline void BaseMapperInternal::write_data(string name, int num_output_files){
    hash<string> hash_fn;

    for(int count = 0; count < num_output_files; count++){
        file_names.push_back(name + "_R" + std::to_string(count));
    }

    for(auto pair : pairs){
        int index = (hash_fn(pair.first) % num_output_files);
        ofstream interm_file(file_names[index],std::ios::app);
        interm_file << pair.first << ':' << pair.second.front();
        for(int vec_cnt = 1; vec_cnt < pair.second.size(); vec_cnt++ ) {
            interm_file << ',' << vec_cnt;
        }
        interm_file << endl;
    }
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const string& key, const string& value);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
        void group_keys();

        string final_file;
        vector<string> interm_files;
        map<string, vector<string>> pairs;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const string& key, const string& value) {
    ofstream fout(final_file, std::ios::app);
	fout <<  key << ", " << value << endl;
}

inline void BaseReducerInternal::group_keys() {
    for(auto file_name : interm_files){
        ifstream temp_file(file_name);

        for(string line; std::getline(temp_file, line); ) {
            istringstream sin(line);
            string key;
            std::getline(sin, key, ':');

            auto pos = pairs.find(key);
            if( pos == pairs.end()){
                vector<string> vec;
                pairs.emplace(key,vec);
            }
            
            for(string value; std::getline(sin,value,','); ) {
                pairs.at(key).push_back(value);
            }
            
        }
    }
}
