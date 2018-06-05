cmake_minimum_required(VERSION 3.8)
project(EX_3)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES MapReduceClient.h MapReduceFramework.h
        MapReduceFramework.cpp eurovisionClient.cpp Barrier.cpp
        Barrier.h
        )
add_executable(EX_3 ${SOURCE_FILES})
TARGET_LINK_LIBRARIES(EX_3 pthread)
