// This file contains the implementation of the camera class. What the camera sees is what printed on the screen
// It is also where the rays for ray tracing originate
// Refer to the documentation for technical and mathematical details

#ifndef CAMERA_H
#define CAMERA_H

#include "ray.h"

// the camera class
class camera {
private:
    vec3 origin;
    vec3 u, v, w;
    vec3 horizontal;
    vec3 vertical;
    vec3 lower_left_corner;
    float len_radius;
    float time0, time1;

public :

    // constructor
    // refer to documentation for technical details
    camera(vec3 lookfrom, vec3 lookat, vec3 vup, float vfov, float aspect, float aperture, float focus_dist, float t0, float t1) {
        time0 = t0;
        time1 = t1;
        len_radius = aperture / 2;
        float theta = vfov * M_PI / 180;
        float half_height = tan(theta / 2);
        float half_width = aspect * half_height;
        origin = lookfrom;

        w = unit_vector(lookfrom - lookat);
        u = unit_vector(cross(vup, w));
        v = cross(w, u);

        lower_left_corner = origin - half_width * focus_dist * u - half_height * focus_dist * v - focus_dist * w;
        horizontal = 2 * half_width * focus_dist * u;
        vertical = 2 * half_height * focus_dist * v;
    }

    // emit a ray from the camera
    ray get_ray(float s, float t) {
        vec3 rd = len_radius * random_in_unit_disk();
        vec3 offset = u * rd.x() + v * rd.y();
        float time = time0 + drand48() * (time1 - time0);
        return ray(origin + offset, lower_left_corner + s * horizontal + t * vertical - origin - offset, time);
    }

    // get a random point for monte-carlo computation.
    vec3 random_in_unit_disk() {
        vec3 p;
        do {
            p = 2.0 * vec3(drand48(), drand48(), 0) - vec3(1, 1, 0);
        } while (dot(p, p) >= 1.0);
        return p;
    }

};

#endif //CAMERA_H
