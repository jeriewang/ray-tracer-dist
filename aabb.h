// This file contains implementation of aabb
// It is an algorithm that calculates the compound bounding box of a list of hitables
// Refer to the documentation for technical and mathematical details

#ifndef AABB_H
#define AABB_H

#include "vec3.h"
#include "ray.h"

// class declaration of aabb
class aabb
{
private:
    vec3 _min;
    vec3 _max;

public:
    aabb(){}

    // constructor, setup caching
    aabb(const vec3 a,const vec3 &b)
    {
        _min = a;_max = b;
    }
    vec3 min()const{ return  _min;}
    vec3 max()const{ return  _max;}
    
    bool hit(const ray& r,float tmin,float tmax)const
    {
        for(int a =0;a<3;a++)
        {
            float invD = 1.0f/r.direction()[a];
            float t0 = (min()[a] - r.direction()[a]) * invD;
            float t1 = (max()[a] - r.direction()[a]) * invD;

            if(invD<0.0f)
                std::swap(t0,t1);
            tmin = t0>tmin?t0:tmin;
            tmax = t1<tmax?t1:tmax;
            if(tmax <= tmin)
                return false;
        }
        return true;
    }
};

//calculates the compound minimal bounding box, works like convex hull
aabb surrounding_box(aabb box0, aabb box1) {
    vec3 small( fmin(box0.min().x(), box1.min().x()),
                fmin(box0.min().y(), box1.min().y()),
                fmin(box0.min().z(), box1.min().z()));
    vec3 big  ( fmax(box0.max().x(), box1.max().x()),
                fmax(box0.max().y(), box1.max().y()),
                fmax(box0.max().z(), box1.max().z()));
    return aabb(small,big);
}

#endif
