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


// Main function. All detail for rendering are implemented in the header.
// Here are scene configuration as well as camera configuration
int main(int argc, char **argv) {

    if (argc != 6) {
        printf("Usage:\n");
        printf("Ray_Tracer x_coordinate y_coordinate sample_rate img_width img_height");
        return -1;
    }

    int sample_rate, x, y, width, height;

    x=atoi(argv[1]);
    y=atoi(argv[2]);
    sample_rate=atoi(argv[3]);
    width=atoi(argv[4]);
    height=atoi(argv[5]);

    y=height-y;

    // Camera View
    vec3 lookfrom(500, 500, -1300);
    vec3 lookat(500, 500, 1000);
    float dist_to_focus = 10.0;
    float aperture = 0.0;
    float vfov = 40.0;
    camera cam(lookfrom, lookat, vec3(0, 1, 0), vfov, float(width) / float(height), aperture, dist_to_focus, 0.0, 1.0);

    hitable *world = scene();

    random_device rd;


    vec3 col(0, 0, 0);
    for (int s = 0; s < sample_rate; s++) {
        float u = float(x + drand48()) / float(width);
        float v = float(y + drand48()) / float(height);

        ray r = cam.get_ray(u, v);
        vec3 p = r.point_at_parameter(2.0);
        vec3 temp = color(r, world, 0);
        temp = de_nan(temp);
        col += temp;
    }

    // average the color
    col /= float(sample_rate);
    col = vec3(sqrt(col[0]), sqrt(col[1]), sqrt(col[2]));

    int ir = int(255.99 * col[0]);
    int ig = int(255.99 * col[1]);
    int ib = int(255.99 * col[2]);
    // r,g,b value can be larger than 255. When over 255, default to % 255
    ir = ir > 255 ? 255 : ir;
    ig = ig > 255 ? 255 : ig;
    ib = ib > 255 ? 255 : ib;


    printf("%i %i %i", ir, ig, ib);

    return 0;
}



