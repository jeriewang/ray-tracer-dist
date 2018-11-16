//
// ray.h
//

#ifndef RAY_H
#define RAY_H


#include "vec3.h"

class ray // the ray object
{
private:
    float _time;
public:
    ray() {}

    ray(const vec3 &a, const vec3 &b, float ti = 0.0) {
        A = a;
        B = b;
        _time = ti;
    }

    vec3 origin() const { return A; } // compute the origin of the ray
    vec3 direction() const { return B; } // compute the direction of the ray
    vec3 point_at_parameter(float t) const {
        return A + t * B;
    } // compute the point at a specific parameter t (linear combination)

    float time() const { return _time; } // compute the time spent for the ray to hit
    vec3 A;
    vec3 B;

};

#endif //RAY_H
