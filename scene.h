// this file contains the scene to be rendered

#ifndef RAY_TRACER_SCENE_H
#define RAY_TRACER_SCENE_H

#include "hitable.h"
#include "material.h"
#include "aarect.h"
#include "box.h"
#include "sphere.h"

// convert rgb value to a vec3 bounded by [0.0,1.0]
vec3 rgb(float r, float g, float b) {
    return vec3(r / 255.0, g / 255.0, b / 255.0);
}


// Scene Construction
hitable *scene() {
    int i = 0;
    hitable **list = new hitable *[25];
    material *rightWall = new lambertian(new constant_texture(rgb(0xb0, 0x7a, 0x29)));

    material *ceiling = new lambertian(new constant_texture(rgb(0xff, 0xe8, 0xe0)));
    material *ground = new metal(rgb(0xff, 0xe8, 0xe0), 0.25);
    material *backWall = new lambertian(new constant_texture(rgb(245, 208, 184)));

    material *leftWall = new lambertian(new constant_texture(rgb(0x60, 0x4e, 0xc9)));
    material *pillar = new isotropic(new constant_texture(rgb(128, 128, 128)));
    material *beacon = new diffuse_light(new constant_texture(rgb(53, 89, 180) * 17.5));
    material *smoke = new isotropic(new constant_texture(rgb(255, 255, 255)));
    material *noise = new lambertian(new noise_texture(0.1));
    material *metal_ = new metal(vec3(0.5, 0.5, 0.5), 0);
    material *glass = new dielectric(1.8);

    //constructing the cornell box
    list[i++] = new flip_normals(new yz_rect(-1400, 1000, -1400, 1000, 1000, leftWall)); //left wall
    list[i++] = new yz_rect(-1400, 1000, -1400, 1000, 0, rightWall); // right wall
    //list[i++] = new xz_rect(375, 625, 375, 625, 999, light); // ceiling light
    list[i++] = new flip_normals(new xz_rect(-1400, 1000, -1400, 1000, 1000, ceiling)); // ceiling
    list[i++] = new xz_rect(-1400, 1000, -1400, 1000, 0, ground); // // ground
    list[i++] = new flip_normals(new xy_rect(0, 1000, 0, 1000, 1000, backWall)); // front wall
    list[i++] = new xy_rect(-0, 1000, 0, 1000, -1350, backWall); // back wall (behind the camera)
    //Done construction

    //---- The centeral pillar
    list[i++] = new translate(new rotate_y(new box(vec3(-150, 290, -150), vec3(150, 300, 150), glass), 45),
                              vec3(500, 0, 500)); //top
    list[i++] = new translate(new rotate_y(new box(vec3(-150, 0, -150), vec3(-140, 300, 150), glass), 45),
                              vec3(500, 0, 500)); //left
    list[i++] = new translate(new rotate_y(new box(vec3(140, 0, -150), vec3(150, 300, 150), glass), 45),
                              vec3(500, 0, 500)); //right
    list[i++] = new translate(new rotate_y(new box(vec3(-150, 0, 140), vec3(150, 300, 150), glass), 45),
                              vec3(500, 0, 500)); //front
    list[i++] = new translate(new rotate_y(new box(vec3(-150, 0, -150), vec3(150, 300, -140), glass), 45),
                              vec3(500, 0, 500)); //back
    list[i++] = new translate(new rotate_y(new box(vec3(-140, 0, -140), vec3(140, 290, 140), pillar), 45),
                              vec3(500, 0, 500)); // the pillar

    list[i++] = new box(vec3(425, 0, 425), vec3(575, 290, 575), beacon); // light source
    list[i++] = new sphere(vec3(500, 290, 500), 100, glass); // center sphere
    list[i++] = new sphere(vec3(500, 290, 500), 50, smoke); // the center sphere
    for (int j = 0; j < 5; j++) {
        list[i++] = new sphere(vec3(500, 290, 500), 60 + j * 5, new dielectric(pow(2, j + 1)));
    }
    //---- Done Construction


    list[i++] = new sphere(vec3(200, 100, 750), 100, metal_); // the metal sphere
    list[i++] = new sphere(vec3(800, 100, 250), 100, noise); // the noise sphere

    list[i++] = new sphere(vec3(750, 750, 750), 150, glass); // the glass sphere
    list[i++] = new sphere(vec3(250, 750, 250), 150, glass); // the glass sphere


    return new hitable_list(list, i);

}

#endif //RAY_TRACER_SCENE_H
