import type ReflectionProbe from "./ReflectionProbe";
import { THREE } from "./VTHREE";

// credits for the box-projecting shader code go to codercat (https://codercat.tk)

type Shader = THREE.WebGLProgramParametersWithUniforms;

function serializeVector3(vector: THREE.Vector3) {
  return { x: vector.x, y: vector.y, z: vector.z };
}

function serializeArray(data: {
  start: THREE.Vector3;
  end: THREE.Vector3;
  normal: THREE.Vector3;
  name: string;
}[]) {
  return JSON.stringify(data.map(item => ({
    start: serializeVector3(item.start),
    end: serializeVector3(item.end),
    normal: serializeVector3(item.normal),
    name: item.name
  })));
}

function deserializeVector3(obj: { x: number; y: number; z: number; }) {
  return new THREE.Vector3(obj.x, obj.y, obj.z);
}

function deserializeArray(jsonobj: {
  start: { x: number; y: number; z: number; };
  end: { x: number; y: number; z: number; };
  normal: { x: number; y: number; z: number; };
  name: string;
}[]) {
  return jsonobj.map(item => ({
    start: deserializeVector3(item.start),
    end: deserializeVector3(item.end),
    normal: deserializeVector3(item.normal),
    name: item.name
  }));
}

const detectWallOnScene = (scene: THREE.Scene, probes: ReflectionProbe[], step = 0.1, detectDist = 0.5) => {

  const meshes: THREE.Mesh[] = [];
  scene.traverse((child) => {
    if ((child as THREE.Mesh).isMesh) {
      meshes.push(child as THREE.Mesh);
    }
  },);

  console.log(meshes);

  // normal : 박스 바깥방향
  // { start:THREE.Vector3; end:THREE.Vector3; normal:THREE.Vector3; name:string; }
  const wallCandidates: { start: THREE.Vector3; end: THREE.Vector3; normal: THREE.Vector3; name: string; }[] = [];

  const start = performance.now();
  probes.forEach((probe, i) => {
    console.log(`[${i + 1}/${probes.length}]`);
    const box = probe.getBox();
    const min = box.min;
    const max = box.max;
    const probeName = probe.getName();

    console.log("probeName", probe.getName(), probeName, box.min, box.max);

    // 중점
    const y = (max.y + min.y) * 0.5;

    // lb - rb
    if (!false) {
      let curx = min.x;
      let normal = new THREE.Vector3(0, 0, -1);
      let wallStart = null;
      while (curx < max.x) {
        const origin = new THREE.Vector3(curx, y, min.z);
        const raycaster = new THREE.Raycaster(origin, normal, 0, detectDist);
        const intersects = raycaster.intersectObjects(meshes);

        // 벽이 있음
        if (intersects.length > 0) {
          if (wallStart === null) {
            wallStart = origin;
          }
        } else {
          // 벽이 없음
          if (wallStart !== null) {
            //이전에 벽이 있었으니 벽이 끝난 경우
            wallCandidates.push({
              start: wallStart,
              end: new THREE.Vector3(curx, y, min.z),
              normal,
              name: probeName,
            });
            wallStart = null;
          }
        }
        curx += step;
      }
      // handle max.x
      if (wallStart !== null) {
        wallCandidates.push({
          start: wallStart,
          end: new THREE.Vector3(max.x, y, min.z),
          normal,
          name: probeName,
        });
      }
    }

    // rb - rt
    if (!false) {
      let curz = min.z;
      let normal = new THREE.Vector3(1, 0, 0);
      let wallStart = null;
      while (curz < max.z) {
        const origin = new THREE.Vector3(max.x, y, curz);
        const raycaster = new THREE.Raycaster(origin, normal, 0, detectDist);
        const intersects = raycaster.intersectObjects(meshes);

        // 벽이 있음
        if (intersects.length > 0) {

          if (wallStart === null) {
            wallStart = origin;
          }
          // 이 외의 경우는 벽이 지속되는 경우

        } else {
          // 벽이 없음
          if (wallStart !== null) {
            //이전에 벽이 있었으니 벽이 끝난 경우
            wallCandidates.push({
              start: wallStart,
              end: new THREE.Vector3(max.x, y, curz),
              normal,
              name: probeName,
            });
            wallStart = null;
          }
          // 이 외의 경우는 벽이 없음이 지속되는 경우
        }
        curz += step;
      }
      // handle max.z
      if (wallStart !== null) {
        wallCandidates.push({
          start: wallStart,
          end: new THREE.Vector3(max.x, y, max.z),
          normal,
          name: probeName,
        });
      }
    }

    // rt - lt
    if (!false) {
      let curx = max.x;
      let normal = new THREE.Vector3(0, 0, 1);
      let wallStart = null;
      while (curx > min.x) {
        const origin = new THREE.Vector3(curx, y, max.z);
        const raycaster = new THREE.Raycaster(origin, normal, 0, detectDist);
        const intersects = raycaster.intersectObjects(meshes);

        // 벽이 있음
        if (intersects.length > 0) {
          if (wallStart === null) {
            wallStart = origin;
          }
        } else {
          // 벽이 없음
          if (wallStart !== null) {
            //이전에 벽이 있었으니 벽이 끝난 경우
            wallCandidates.push({
              start: wallStart,
              end: new THREE.Vector3(curx, y, max.z),
              normal,
              name: probeName,
            });
            wallStart = null;
          }
        }
        curx -= step;
      }
      // handle min.x
      if (wallStart !== null) {
        wallCandidates.push({
          start: wallStart,
          end: new THREE.Vector3(min.x, y, max.z),
          normal,
          name: probeName,
        });
      }
    }

    // lt - lb
    if (!false) {
      let curz = max.z;
      let normal = new THREE.Vector3(-1, 0, 0);
      let wallStart = null;
      while (curz > min.z) {
        const origin = new THREE.Vector3(min.x, y, curz);
        const raycaster = new THREE.Raycaster(origin, normal, 0, detectDist);
        const intersects = raycaster.intersectObjects(meshes);

        // 벽이 있음
        if (intersects.length > 0) {
          if (wallStart === null) {
            wallStart = origin;
          }
        } else {
          // 벽이 없음
          if (wallStart !== null) {
            //이전에 벽이 있었으니 벽이 끝난 경우
            wallCandidates.push({
              start: wallStart,
              end: new THREE.Vector3(min.x, y, curz),
              normal,
              name: probeName,
            });
            wallStart = null;
          }
        }
        curz -= step;
      }
      // handle min.z
      if (wallStart !== null) {
        wallCandidates.push({
          start: wallStart,
          end: new THREE.Vector3(min.x, y, min.z),
          normal,
          name: probeName,
        });
      }
    }


  })

  const elapsed = performance.now() - start;
  console.log(`elapsed time : ${elapsed} ms`);

  return wallCandidates;
}


const boxProjectDefinitions = /*glsl */`
#ifdef V_ENV_MAP
    varying vec3 _vWorldPosition;
    
    #define lengthSquared(v) (dot((v), (v)))

    vec3 parallaxCorrectNormal( vec3 v, vec3 cubeSize, vec3 cubePos ) {
        vec3 nDir = normalize( v );

        vec3 rbmax = ( .5 * cubeSize + cubePos - _vWorldPosition ) / nDir;
        vec3 rbmin = ( -.5 * cubeSize + cubePos - _vWorldPosition ) / nDir;

        vec3 rbminmax;

        rbminmax.x = ( nDir.x > 0. ) ? rbmax.x : rbmin.x;
        rbminmax.y = ( nDir.y > 0. ) ? rbmax.y : rbmin.y;
        rbminmax.z = ( nDir.z > 0. ) ? rbmax.z : rbmin.z;

        // 월드좌표의 반사벡터가 박스에서 얼마만한 강도로 반사될 지 정해주는 계수
        float correction = min( min( rbminmax.x, rbminmax.y ), rbminmax.z );
        vec3 boxIntersection = _vWorldPosition + nDir * correction;
        // vec3 boxIntersection = _vWorldPosition + nDir;
        
        vec3 retval = boxIntersection - cubePos;
        // retval.x = -retval.x;

        return retval;
    }

    float distanceToAABB(vec3 point, vec3 boxCenter, vec3 boxSize) {
        vec3 boxMin = boxCenter - boxSize * 0.5;
        vec3 boxMax = boxCenter + boxSize * 0.5;
        
        vec3 closestPoint = clamp(point, boxMin, boxMax);
        return lengthSquared(point - closestPoint);
    }

    vec4 probeColor(vec3 worldReflectVec, int i, float roughness) {
        
        vec3 probeCenter = uProbe[i].center;
        vec3 probeSize = uProbe[i].size;

        mat3 _envMapRotation = mat3(1.0);
        vec3 localReflectVec = _envMapRotation * parallaxCorrectNormal( worldReflectVec, probeSize, probeCenter );


        vec4 envMapColor = vec4(0.0);

        if(i == 0){

            envMapColor += textureCube( uProbeTextures[0], localReflectVec, roughness );

        }
        #if PROBE_COUNT > 1
        else if( i == 1){

            envMapColor += textureCube( uProbeTextures[1], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 2
        else if( i == 2){

            envMapColor += textureCube( uProbeTextures[2], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 3
        else if( i == 3){

            envMapColor += textureCube( uProbeTextures[3], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 4
        else if( i == 4){

            envMapColor += textureCube( uProbeTextures[4], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 5
        else if( i == 5){

            envMapColor += textureCube( uProbeTextures[5], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 6
        else if( i == 6){

            envMapColor += textureCube( uProbeTextures[6], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 7
        else if( i == 7){

            envMapColor += textureCube( uProbeTextures[7], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 8
        else if( i == 8){

            envMapColor += textureCube( uProbeTextures[8], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 9
        else if( i == 9){

            envMapColor += textureCube( uProbeTextures[9], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 10
        else if( i == 10){

            envMapColor += textureCube( uProbeTextures[10], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 11
        else if( i == 11){

            envMapColor += textureCube( uProbeTextures[11], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 12
        else if( i == 12){

            envMapColor += textureCube( uProbeTextures[12], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 13
        else if( i == 13){

            envMapColor += textureCube( uProbeTextures[13], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 14
        else if( i == 14){

            envMapColor += textureCube( uProbeTextures[14], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 15
        else if( i == 15){

            envMapColor += textureCube( uProbeTextures[15], localReflectVec, roughness );

        }
        #endif
        #if PROBE_COUNT > 16
        else if( i == 16){

            envMapColor += textureCube( uProbeTextures[16], localReflectVec, roughness );

        }
        #endif
        // WebGL GLSL스펙 상 최대 텍스쳐 갯수는 16이므로 여기서 끝
        else {

            envMapColor = vec4(0.0);
        }
        return envMapColor;
    }

    #ifdef V_ENV_MAP_FLOOR
    bool intersectRaySegment(vec2 p1, vec2 p2, vec2 ro, vec2 rd, out vec2 intersection) {
      vec2 v1 = ro - p1;
      vec2 v2 = p2 - p1;
      vec2 v3 = vec2(-rd.y, rd.x); // 광선의 법선 벡터

      float dotProduct = dot(v2, v3);
      if(abs(dotProduct) < 1e-6f)
          return false; // 광선과 선이 평행함

      float t1 = (v2.x * v1.y - v2.y * v1.x) / dotProduct;
      float t2 = dot(v1, v3) / dotProduct;

      if(t1 >= 0.0f && t2 >= 0.0f && t2 <= 1.0f) {
          intersection = ro + t1 * rd;
          return true;
      }
      return false;
    }
    #endif


#endif
`;

function useMultiProbe(shader:
  Shader, args: {
    uniforms: Record<string, any>,
    defines: Record<string, any>,
    namehash: string,
  }) {

  shader.cleanup?.();

  const { uniforms, defines, namehash } = args;

  shader.defines = shader.defines ?? {};
  delete shader.defines["BOX_PROJECTED_ENV_MAP"];
  shader.defines = {
    ...shader.defines,
    ...defines,
    V_ENV_MAP: 1
  }

  shader.uniforms = shader.uniforms ?? {};
  shader.uniforms = {
    ...shader.uniforms,
    ...uniforms,
  }

  shader.cleanup = () => {
    if (shader.defines) {
      const defineKeys = [
        ...Object.keys(defines),
        "V_ENV_MAP"
      ];
      defineKeys.forEach(key => {
        delete shader.defines![key];
      })
    }

    if (shader.uniforms) {
      const uniformKeys = [
        ...Object.keys(uniforms)
      ]
      uniformKeys.forEach(key => {
        delete shader.uniforms![key];
      })
    }
  }

  // vertex shader
  shader.vertexShader = "varying vec3 _vWorldPosition;\n" + shader.vertexShader
    .replace(
      "#include <worldpos_vertex>",
      `
      #ifndef USE_ENVMAP
      vec4 worldPosition = modelMatrix * vec4( transformed, 1.0 );
      #endif
      #ifdef V_ENV_MAP
          _vWorldPosition = (modelMatrix * vec4( transformed, 1.0 )).xyz;
      #endif
      #include <worldpos_vertex>
      `
    );

  // fragment shader
  shader.fragmentShader = shader.fragmentShader
    .replace(
      "#include <envmap_physical_pars_fragment>",
      THREE.ShaderChunk.envmap_physical_pars_fragment
    )
    .replace(
      "vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );",
      `
            vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
            #ifdef BOX_PROJECTED_ENV_MAP
                worldNormal = parallaxCorrectNormal( worldNormal, envMapSize, envMapPosition );
            #endif
            `
    )
    .replace(
      "reflectVec = inverseTransformDirection( reflectVec, viewMatrix );",
      `
            reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
            #ifdef BOX_PROJECTED_ENV_MAP
                reflectVec = parallaxCorrectNormal( reflectVec, envMapSize, envMapPosition );
            #endif
            `
    );

  shader.fragmentShader = shader.fragmentShader.replace("void main()",
    `
        #ifdef V_ENV_MAP
        struct Probe {
            vec3 center;
            vec3 size;
            // samplerCube cubeTexture;
            // sampler2D envTexture;
        };
        struct Wall {
            vec3 start;
            vec3 end;
            int index; // 프로브 인덱스, 0부터 PROBE_COUNT-1까지
        };
        uniform Probe _uProbe${namehash}[PROBE_COUNT];
        uniform samplerCube _uProbeTextures${namehash}[PROBE_COUNT];
        uniform float _uProbeIntensity${namehash};
        
        #ifdef V_ENV_MAP_FLOOR
        uniform Wall _uWall${namehash}[WALL_COUNT];
        uniform float uProbeBlendDist;
        #endif

        #endif

${boxProjectDefinitions}
void main()
        `
  ).replace("#include <lights_fragment_end>",
        /** glsl */`
    #ifdef V_ENV_MAP
        
        float roughness = material.roughness;
        
        float weights[PROBE_COUNT];
        float wTotal = 0.0;
        
        // 표면에서 반사되는 벡터를 월드좌표계에서 본 것
        vec3 worldReflectVec = reflect( - geometryViewDir, geometryNormal );
        worldReflectVec = normalize(worldReflectVec);
        worldReflectVec = inverseTransformDirection( worldReflectVec, viewMatrix );

        float reflectWeight = 1.0;
        float distWeight = 1.0;

        float dists[PROBE_COUNT];
        float distTotal = 0.0;

        ////////////////////////////////////////////////
        // 각 프로브까지의 거리를 계산
        #pragma unroll_loop_start
        for (int i = 0; i < PROBE_COUNT; i++) {
          vec3 probeCenter = uProbe[i].center;
          vec3 probeSize = uProbe[i].size;

          float distFromCenter = lengthSquared(_vWorldPosition-probeCenter);
          float distFromBox = distanceToAABB(_vWorldPosition, probeCenter, probeSize);
          
          dists[i] = distFromBox;
          
          distTotal += dists[i];
        }
        #pragma unroll_loop_end

        ////////////////////////////////////////////////
        // 가장 가까운 프로브 고르기
        int minIndex = -1;
        float minDist = 100000.0;
        #pragma unroll_loop_start
        for (int i = 0; i < PROBE_COUNT; i++) {
          if (dists[i] < minDist) {
            minDist = dists[i];
            minIndex = i;
          }
        }
        #pragma unroll_loop_end

        ////////////////////////////////////////////////
        // 프로브로부터 얻은 색상 반영
        vec4 envMapColor = vec4(0.0);

        ////////////////////////////////////////////////
        // case 1. 바닥
        #ifdef V_ENV_MAP_FLOOR
        vec3 localReflectVec = parallaxCorrectNormal( worldReflectVec, uProbe[minIndex].size, uProbe[minIndex].center );

        int closestWallIndex = -1;
        int closestProbeIndex = minIndex;
        float maxDist = uProbeBlendDist * uProbeBlendDist;

        float closestWallDist = maxDist;

        #pragma unroll_loop_start
        for (int i = 0; i < WALL_COUNT; ++i) {
            vec2 start = uWall[i].start.xz;
            vec2 end = uWall[i].end.xz;
            int probeIndex = uWall[i].index;

            vec2 origin = _vWorldPosition.xz;
            vec2 ray = worldReflectVec.xz;
            vec2 intersection = vec2(0.0);

            if(intersectRaySegment(start, end, origin, ray, intersection)){
                
            float dist = lengthSquared(intersection - origin);

                if(dist < closestWallDist){
                    closestWallDist = dist;
                    closestWallIndex = i;
                    closestProbeIndex = probeIndex;
                }
            }
        }    
        #pragma unroll_loop_end

        envMapColor = probeColor(worldReflectVec, closestProbeIndex, roughness);

        #endif

        #ifndef V_ENV_MAP_FLOOR

        ////////////////////////////////////////////////
        // case2. 바닥이 아닌 여러 개의 프로브가 적용되는 경우
        // 그냥 제일 가까운 프로브 반사
        envMapColor = probeColor(worldReflectVec, minIndex, roughness);
        // envMapColor = vec4(1.0, 0.0, 0.0, 1.0);
            
        #endif

        radiance += clamp(envMapColor.rgb, 0.0, 1.0) * uProbeIntensity;

        #endif
        ` + "#include <lights_fragment_end>"
  )

  const showVWorldPosition = false;
  if (showVWorldPosition) {
    shader.fragmentShader = shader.fragmentShader.replace("#include <dithering_fragment>", `#include <dithering_fragment>
        gl_FragColor.rgb = _vWorldPosition;
        `)
  }

  const downloadShader = () => {
    // download vertex & frament shader using a tag
    // const a = document.createElement("a");
    // a.href = URL.createObjectURL(new Blob([shader.vertexShader], { type: "text/plain" }));
    // a.download = "vertex.glsl";
    // a.click();

    const b = document.createElement("a");
    b.href = URL.createObjectURL(new Blob([shader.fragmentShader], { type: "text/plain" }));
    b.download = "fragment.glsl";
    b.click();
  }
  // downloadShader()
}

const wallFileName = "walls.json"
let __wals: null | any = null;
export const getWalls = async (scene?: THREE.Scene, probes?: ReflectionProbe[]): Promise<{
  start: THREE.Vector3;
  end: THREE.Vector3;
  normal: THREE.Vector3;
  name: string;
}[]> => {
  if (!__wals) {
    __wals = fetch("/" + wallFileName).then(res => res.json()).then(deserializeArray).catch(e => {
      console.error(e);
      debugger;
      if (!scene || !probes) {
        throw new Error("벽감지를 위해 scene, probes를 넣거나 walls.json 필요")
      }

      const walls = detectWallOnScene(scene, probes, 0.05);

      const jsonobj = serializeArray(walls);

      // save it
      const filanme = wallFileName;
      const a = document.createElement("a");
      const file = new Blob([jsonobj], { type: "application/json" });
      a.href = URL.createObjectURL(file);
      a.download = filanme;
      a.click();

      return walls;
    })
  }
  return __wals;
}

function hashStringArray(arr: string[]) {
  let hash = 0;
  const prime = 31; // 작은 소수를 사용하여 충돌을 줄임

  for (let str of arr) {
    for (let i = 0; i < str.length; i++) {
      hash = (hash * prime + str.charCodeAt(i)) % 1_000_000_007;
    }
  }

  return hash.toString(16); // 16진수 문자열로 변환
}

function removeTrailingThreeDigitNumber(str: string) {
  return str.replace(/\.\d{3}$/, '');
}

export const drawWalls = (walls: {
  start: THREE.Vector3;
  end: THREE.Vector3;
  normal: THREE.Vector3;
  name: string;
}[], scene: THREE.Scene, normalLength = 0.2) => {
  walls.forEach((wall, i) => {
    const { start, end, normal, name } = wall;
    // add line from start to end, then add arrow on middle point using normal

    const points = [];
    points.push(start);
    points.push(end);

    const geometry = new THREE.BufferGeometry().setFromPoints(points);

    // hsl color from index
    const color = new THREE.Color();
    color.setHSL(i / walls.length, 1.0, 0.5);

    const line = new THREE.Line(geometry, new THREE.LineBasicMaterial({ color }));

    scene.add(line);

    const mid = new THREE.Vector3().addVectors(start, end).multiplyScalar(0.5);

    const arrowHelper = new THREE.ArrowHelper(normal, mid, normalLength, color);

    scene.add(arrowHelper);
  })
}

const createMultiProbeShader = (mat: THREE.MeshStandardMaterial, probes: ReflectionProbe[]) => {
  const targetNames = probes.map(p => p.getName());
  // targetNames.sort();
  const namehash = hashStringArray(targetNames);

  mat.defines = mat.defines ?? {};

  // !중요 : 이름을 넣어주지 않으면 캐싱된 셰이더와 헷갈려함
  const SHADER_NAME = removeTrailingThreeDigitNumber(mat.name) + namehash;
  mat.defines.SHADER_NAME = SHADER_NAME

  const metaUniform = probes.map((p, index) => ({
    center: p.getBox().getCenter(new THREE.Vector3()),
    size: p.getBox().getSize(new THREE.Vector3()),
  }))
  const textures = probes.map(p => p.getRenderTargetTexture());

  const uProbe = `_uProbe${namehash}`;
  const uProbeTextures = `_uProbeTextures${namehash}`;
  const uProbeIntensity = `_uProbeIntensity${namehash}`;

  const uniforms = {
    [uProbe]: {
      value: metaUniform
    },
    [uProbeTextures]: {
      value: textures
    },
    [uProbeIntensity]: {
      value: 1.0
    },
  };

  const defines = {
    PROBE_COUNT: probes.length,
    uProbe,
    uProbeTextures,
    uProbeIntensity,
    SHADER_NAME: mat.name
    // SHADER_NAME
  }

  return (shader: Shader) => {
    useMultiProbe(shader, {
      uniforms,
      defines,
      namehash,
    });
  }
}

const createFloorShader = (mat: THREE.MeshStandardMaterial, probes: ReflectionProbe[], walls: {
  start: THREE.Vector3;
  end: THREE.Vector3;
  normal: THREE.Vector3;
  name: string;
}[]) => {

  const targetNames = probes.map(p => p.getName());
  // targetNames.sort();
  const namehash = hashStringArray(targetNames);

  mat.defines = mat.defines ?? {};

  // !중요 : 이름을 넣어주지 않으면 캐싱된 셰이더와 헷갈려함
  const SHADER_NAME = removeTrailingThreeDigitNumber(mat.name) + namehash;
  mat.defines.SHADER_NAME = SHADER_NAME

  const targetWalls = walls.filter(w => targetNames.includes(w.name)).map((wall) => ({
    start: wall.start,
    end: wall.end,
    index: targetNames.indexOf(wall.name)
  }));

  const metaUniform = probes.map((p, index) => ({
    center: p.getBox().getCenter(new THREE.Vector3()),
    size: p.getBox().getSize(new THREE.Vector3()),
  }))
  const textures = probes.map(p => p.getRenderTargetTexture());

  const uProbe = `_uProbe${namehash}`;
  const uProbeTextures = `_uProbeTextures${namehash}`;
  const uProbeIntensity = `_uProbeIntensity${namehash}`;
  const uWall = `_uWall${namehash}`;

  const uniforms = {
    [uProbe]: {
      value: metaUniform
    },
    [uProbeTextures]: {
      value: textures
    },
    [uProbeIntensity]: {
      value: 5.0
    },
    [uWall]: {
      value: targetWalls
    },
    uProbeBlendDist: {
      value: 20.0
    }
  };

  const defines = {
    PROBE_COUNT: probes.length,
    WALL_COUNT: targetWalls.length,
    uProbe,
    uProbeTextures,
    uProbeIntensity,
    uWall,
    SHADER_NAME: mat.name,
    V_ENV_MAP_FLOOR: 1
    // SHADER_NAME
  }

  return (shader: Shader) => {
    useMultiProbe(shader, {
      uniforms,
      defines,
      namehash,
    });
  }
}



export async function applyFloorProbe(material: THREE.MeshStandardMaterial, probes: ReflectionProbe[], walls: {
  start: THREE.Vector3;
  end: THREE.Vector3;
  normal: THREE.Vector3;
  name: string;
}[]) {
  material.onBeforeCompile = createFloorShader(material, probes, walls);
  material.needsUpdate = true;
}

// 입력된 프로브들 중 가장 가까운 프로브를 반사
export async function applyMultiProbe(material: THREE.MeshStandardMaterial, probes: ReflectionProbe[]) {
  material.onBeforeCompile = createMultiProbeShader(material, probes);
  material.needsUpdate = true;
}