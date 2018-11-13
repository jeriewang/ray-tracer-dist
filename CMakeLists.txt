cmake_minimum_required(VERSION 3.8)
project(Ray_Tracer)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES main.cpp vec3.h ray.h hitable.h sphere.h hitable_list.h camera.h material.h aabb.h texture.h perlin.h aarect.h box.h scene.h)
add_executable(Ray_Tracer ${SOURCE_FILES})