// This file contains implementation of all materials
// Refer to the documentation for technical and mathematical details

#ifndef MATERIAL_H
#define MATERIAL_H


struct hit_record;

#include "ray.h"
#include "hitable.h"
#include "texture.h"

// Solve schlick function 
float schlick(float cosine, float ref_idx) {
    float r0 = (1-ref_idx) / (1+ref_idx);
    r0 = r0*r0;
    return r0 + (1-r0)*pow((1 - cosine),5);
}

// compute refraction
bool refract(const vec3& v, const vec3& n, float ni_over_nt, vec3& refracted) {
    vec3 uv = unit_vector(v);
    float dt = dot(uv, n);
    float discriminant = 1.0 - ni_over_nt*ni_over_nt*(1-dt*dt);
    if (discriminant > 0) {
        refracted = ni_over_nt*(uv - n*dt) - n*sqrt(discriminant);
        return true;
    }
    else
        return false;
}

// compute reflection
vec3 reflect(const vec3& v, const vec3& n) {
    return v - 2*dot(v,n)*n;
}


// Return a random point inside the bounding box of the sphere
vec3 random_in_unit_sphere() {
    vec3 p;
    do {
        p = 2.0*vec3(drand48(),drand48(),drand48()) - vec3(1,1,1);
    } while (dot(p,p) >= 1.0);
    return p;
}


class material  {
public:
    //
    virtual bool scatter(const ray& r_in, const hit_record& rec, vec3& attenuation, ray& scattered) const = 0;
    // r_in: input ray
    // rec: ray hitting recording
    // attenuation: vec3 decay
    // scattered: refracted light

    // Object not illuminated nor emits light. Returns black.

    virtual vec3 emitted(float u,float v,const vec3 &p)const {
        return vec3(0,0,0);}
};


class lambertian : public material { //basic lambertian material
public:
    lambertian(texture *color) : albedo(color) {}
    virtual bool scatter(const ray& r_in, const hit_record& rec, vec3& attenuation, ray& scattered) const  {
        vec3 target = rec.p + rec.normal + random_in_unit_sphere();
        scattered = ray(rec.p, target-rec.p, r_in.time());
        attenuation = albedo->value(rec.u, rec.v, rec.p);
        return true;
    }

    texture *albedo;
};

// basic metal.
class metal : public material {
public:
    //constructor. All arguments in range [0,1]
    metal(const vec3& color, float fuzziness) : albedo(color) { if (fuzziness < 1) fuzz = fuzziness; else fuzz = 1; }
    virtual bool scatter(const ray& r_in, const hit_record& rec, vec3& attenuation, ray& scattered) const  {
        vec3 reflected = reflect(unit_vector(r_in.direction()), rec.normal);
        scattered = ray(rec.p, reflected + fuzz*random_in_unit_sphere());
        attenuation = albedo;
        return (dot(scattered.direction(), rec.normal) > 0);
    }
    vec3 albedo;
    float fuzz;
};

// dielectric material that refract lights, such as glass or water
class dielectric : public material {
public:
    dielectric(float ri) : ref_idx(ri) {}
    virtual bool scatter(const ray& r_in, const hit_record& rec, vec3& attenuation, ray& scattered) const  {
        vec3 outward_normal;
        vec3 reflected = reflect(r_in.direction(), rec.normal);
        float ni_over_nt;
        attenuation = vec3(1.0, 1.0, 1.0);
        vec3 refracted;
        float reflect_prob;
        float cosine;
        if (dot(r_in.direction(), rec.normal) > 0) {
            outward_normal = -rec.normal;
            ni_over_nt = ref_idx;
            cosine = dot(r_in.direction(), rec.normal) / r_in.direction().length();
            cosine = sqrt(1 - ref_idx*ref_idx*(1-cosine*cosine));
        }
        else {
            outward_normal = rec.normal;
            ni_over_nt = 1.0 / ref_idx;
            cosine = -dot(r_in.direction(), rec.normal) / r_in.direction().length();
        }
        if (refract(r_in.direction(), outward_normal, ni_over_nt, refracted))
            reflect_prob = schlick(cosine, ref_idx);
        else
            reflect_prob = 1.0;
        // randomly assign whether the light is refracted or reflected
        if (drand48() < reflect_prob)
            scattered = ray(rec.p, reflected);
        else
            scattered = ray(rec.p, refracted);
        return true;
    }

    float ref_idx;
};

//basic light emitting material. Used to create a light source.
class diffuse_light:public material {
public:
    diffuse_light(texture *a) : emit(a) {}

    virtual bool scatter(const ray &r_in, const hit_record &rec, vec3 &attenuation, ray &scattered) const {
        return false;
    }

    virtual vec3 emitted(float u, float v, const vec3 &p) const {
        return emit->value(u, v, p);
    }

    texture *emit;
};

// Basic isotropic material, such as smoke
class isotropic : public material {
public:
    isotropic(texture *a) : albedo(a) {}
    virtual bool scatter(const ray& r_in, const hit_record& rec, vec3& attenuation, ray& scattered) const  {
        scattered = ray(rec.p, random_in_unit_sphere());
        attenuation = albedo->value(rec.u, rec.v, rec.p);
        return true;
    }
    texture *albedo;
};


#endif //MATERIAL_H
