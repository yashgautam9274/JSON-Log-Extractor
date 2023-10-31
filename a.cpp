#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <thread>
#include <mutex>
#include "rapidjson/document.h"

// Define the number of threads
const int numThreads = 4;

// Mutex for thread-safe file I/O
std::mutex fileMutex;
void processElement(const rapidjson::Value& element, const std::string& key, const std::string& outputDir) {
    std::string outputFileName = outputDir + key + ".column";
    std::ofstream outputFile(outputFileName, std::ios::app);

    if (element.IsNull()) {
        outputFile << "\n";
    } else if (element.IsBool()) {
        outputFile << (element.GetBool() ? "true\n" : "false\n");
    } else if (element.IsInt()) {
        outputFile << element.GetInt() << "\n";
    } else if (element.IsUint()) {
        outputFile << element.GetUint() << "\n";
    } else if (element.IsInt64()) {
        outputFile << element.GetInt64() << "\n";
    } else if (element.IsUint64()) {
        outputFile << element.GetUint64() << "\n";
    } else if (element.IsDouble()) {
        outputFile << element.GetDouble() << "\n";
    } else if (element.IsString()) {
        outputFile << element.GetString() << "\n";
    } else {
        std::cerr << "Error: Unsupported data type for key '" << key << "'" << std::endl;
    }

    outputFile.close();
}



void processJSON(const rapidjson::Value& value, const std::string& parentKey, const std::string& outputDir) {
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
        const std::string key = parentKey.empty() ? it->name.GetString() : parentKey + "." + it->name.GetString();
        const rapidjson::Value& element = it->value;

        if (element.IsObject()) {
            // Recursively process nested objects
            processJSON(element, key, outputDir);
        } else {
            // Process leaf elements
            processElement(element, key, outputDir);
        }
    }
}






void processLogEntries(std::ifstream& logFile, const std::string& outputDir) {
    // Reset the file pointer to the beginning of the file
    logFile.clear();
    logFile.seekg(0);

    std::string line;
    while (std::getline(logFile, line)) {
        rapidjson::Document document;
        document.Parse(line.c_str());

        if (document.HasParseError()) {
           // std::cerr << "Error: Invalid JSON entry - " << document.GetParseError() << std::endl;
            continue;
        }

        processJSON(document, "", outputDir);
    }
}




int main() {
    const char* logFilePath = "dummy.log";
    const char* outputDir = "output_columns/";

    // Create the output directory if it doesn't exist
    std::string command = "mkdir -p " + std::string(outputDir);
    system(command.c_str());

    std::ifstream logFile(logFilePath);
    if (!logFile) {
        std::cerr << "Error: Cannot open log file." << std::endl;
        return 1;
    }

    // Create threads
    std::vector<std::thread> threads;
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back(processLogEntries, std::ref(logFile), outputDir);
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    logFile.close();
    return 0;
}
// multiple occurences