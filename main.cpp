#include<fstream>
#include<iostream>
#include "sphere.h"
#include "box.h"
#include "hitable_list.h"
#include "float.h"
#include "camera.h"
#include "random"
#include "material.h"
#include "scene.h"
#include "aarect.h"
#include <math.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <chrono>

#define verbose


using namespace std;

vec3 color(const ray &r, hitable *world, int depth) {
    // calculate the color of a ray
    hit_record rec;
    if (world->hit(r, 0.001, MAXFLOAT, rec)) {
        // Light after scattering
        ray scattered;
        // Light attenuation
        vec3 attenuation;
        // Calculate the color of the origin of light
        vec3 emitted = rec.mat_ptr->emitted(rec.u, rec.v, rec.p);
        if (depth < 50 && rec.mat_ptr->scatter(r, rec, attenuation, scattered)) {
            // regression
            return emitted + attenuation * color(scattered, world, depth + 1);
        } else {
            return emitted;
        }
    } else {
        return vec3(0, 0, 0);
    }
}


// convert a NaN result to a usable result
// refer to documentation for details
inline vec3 de_nan(const vec3 &c) { //
    vec3 t = c;
    if (!(t[0] == t[0]))
        t[0] = 0;
    if (!(t[1] == t[1]))
        t[1] = 0;
    if (!(t[2] == t[2]))
        t[2] = 0;
    return t;
}

const int processesCount = 8; // how many worker processes to spawn

// Create processes so the computation can be spread out to take advantage of multiple CPUs
int createProcess() {
    int id = 0;
    for (int level = log2(processesCount); level > 0; level--) {
        if (fork() == 0)
            id += pow(2, level - 1);
    }
#ifdef verbose
    printf("\033[s\033[%dAProcess %d created\033[u", processesCount - id, id);
#endif
    return id;
}

// Main function. All detail for rendering are implemented in the header.
// Here are scene configuration as well as camera configuration
int main(int argc, char **argv) {
    // pixel count (x,y)
    const int nx = 4096;
    const int ny = 4096;
    // Sampling Size
    const int ns = 100000;
    // Camera View
    vec3 lookfrom(500, 500, -1300);
    vec3 lookat(500, 500, 1000);
    float dist_to_focus = 10.0;
    float aperture = 0.0;
    float vfov = 40.0;
    camera cam(lookfrom, lookat, vec3(0, 1, 0), vfov, float(nx) / float(ny), aperture, dist_to_focus, 0.0, 1.0);

    hitable *world = scene();

    random_device rd;
    int distribution_count, distribution_index;

    if (argc >= 3) {
        sscanf(argv[1], "%d", &distribution_count);
        sscanf(argv[2], "%d", &distribution_index);
    } else {
        printf("Distribution count?: ");
        scanf("%d", &distribution_count);
        printf("Distribution index?: ");
        scanf("%d", &distribution_index);
    }
    int distributionSliceBegin = ny / distribution_count * distribution_index;
    int distributionSliceRange = ny / distribution_count;

    ofstream mainFile("img.ppm");
    mainFile << "P3\n" << nx << " " << ny << "\n255\n";
    mainFile.close(); // create the main file

    printf("%s", "\033[2J");
    for (int i = 0; i < processesCount; i++) {
        printf("%s", "\n");
    }

    //spawn workers and calculate there respective assignments
    int workerID = createProcess();
    int workerBegin = distributionSliceRange / processesCount * (workerID + 1) + distributionSliceBegin;
    int workerEnd = distributionSliceRange / processesCount * workerID + distributionSliceBegin;

#ifdef verbose // use compiler macro to reduce runtime calculation
    printf("\033[s\033[%dAWorker %d: row %d-row%d\033[u", processesCount * 2 - workerID + 1, workerID, workerEnd,
           workerBegin - 1);
#endif

#ifndef verbose
    printf("Worker %d: row %d-row%d\n", workerID, workerEnd, workerBegin - 1);
#endif

    auto start = chrono::system_clock::now();
    for (int j = workerBegin - 1; j >= workerEnd; j--) {

        char fileName[15];
        sprintf(fileName, "imgRow%d", j);
        ofstream OutFile(fileName);

        for (int i = 0; i < nx; i++) {
            vec3 col(0, 0, 0);
#ifdef verbose // use compiler macro to reduce runtime calculation
            printf("\033[s\033[%dA\033[KWorker %d: %d/%d\033[u", processesCount - workerID, workerID,
                   (workerBegin - j - 1) * nx + i, (workerBegin - workerEnd) * nx);
            cout.flush();
#endif
            for (int s = 0; s < ns; s++) {
                float u = float(i + drand48()) / float(nx);
                float v = float(j + drand48()) / float(ny);

                ray r = cam.get_ray(u, v);
                vec3 p = r.point_at_parameter(2.0);
                vec3 temp = color(r, world, 0);
                temp = de_nan(temp);
                col += temp;
            }

            // average the color
            col /= float(ns);
            col = vec3(sqrt(col[0]), sqrt(col[1]), sqrt(col[2]));

            int ir = int(255.99 * col[0]);
            int ig = int(255.99 * col[1]);
            int ib = int(255.99 * col[2]);
            // r,g,b value can be larger than 255. When over 255, default to % 255
            ir = ir > 255 ? 255 : ir;
            ig = ig > 255 ? 255 : ig;
            ib = ib > 255 ? 255 : ib;

            string s = to_string(ir) + " " + to_string(ig) + " " + to_string(ib) + "\n";
            OutFile << s;

        }
#ifndef verbose
        printf("Row %d completed\n", j);
#endif
        OutFile.close();
    }
#ifdef verbose // use compiler macro to reduce runtime calculation
    printf("\033[s\033[%dA\033[KWorker %d: completed (%f s)\033[u", processesCount - workerID, workerID,
           (chrono::system_clock::now() - start).count() / pow(10, 6));
#endif

    cout.flush();

}

