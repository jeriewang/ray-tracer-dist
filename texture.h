// This file contains all the classes and functions necessary to constuct the textures
// Refer to the documentation for technical and mathematical details

#ifndef TEXTURE_H
#define TEXTURE_H

#include "vec3.h"
#include "perlin.h"

// texture base class
class texture {
public:
    virtual vec3 value(float u, float v, const vec3 &p) const = 0;
};

// constant texture. Just solid color block
class constant_texture : public texture {
public:
    constant_texture() {}

    constant_texture(vec3 c) : color(c) {}

    virtual vec3 value(float u, float v, const vec3 &p) const {
        return color;
    }

    vec3 color;
};


// noise texture using perlin noise.
// Refer to documentation for technical details
class noise_texture : public texture {
public:
    noise_texture() {}

    noise_texture(float sc) : scale(sc) {}

    virtual vec3 value(float u, float v, const vec3 &p) const {
        // add disturbance and scaling
        return vec3(1, 1, 1) * 0.5 * (1 + sin(scale * p.x() + 5 * noise.turb(scale * p)));
    }

    perlin noise;
    float scale;
};


#endif //TEXTURE_H
