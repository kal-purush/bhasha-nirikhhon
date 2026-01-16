'use strict';

/**
 * Car class
 * This is a HTML/Javascript adaptation of Marco Monster's 2D car physics demo.
 * Physics Paper here:
 * http://www.asawicki.info/Mirror/Car%20Physics%20for%20Games/Car%20Physics%20for%20Games.html
 * Windows demo written in C here:
 * http://www.gamedev.net/topic/394292-demosource-of-marco-monsters-car-physics-tutorial/
 * Additional ideas from here:
 * https://github.com/Siorki/js13kgames/tree/master/2013%20-%20staccato
 *
 * Adapted by Mike Linkovich
 * http://www.spacejack.ca/projects/carphysics2d/
 * https://github.com/spacejack/carphysics2d
 *
 * License: MIT
 * http://opensource.org/licenses/MIT
 */
class Car {
    //  Car state variables
    public heading = 0.0;  // angle car is pointed at (radians)
    public position = new THREE.Vector2();  // metres in world coords
    public velocity = new THREE.Vector2();  // m/s in world coords
    public velocityLocal = new THREE.Vector2();  // m/s in local car coords (x is forward y is sideways)
    public acceleration = new THREE.Vector2();  // acceleration in world coords
    public accelerationLocal = new THREE.Vector2();   // accleration in local car coords
    public absoluteVelocity = 0.0;  // absolute velocity m/s
    public yawRate = 0.0;   // angular velocity in radians
    public steerInput = 0.0;	// amount of steering input (-1.0..1.0)
    public steerAngle = 0.0;  // actual front wheel steer angle (-maxSteer..maxSteer)
    public inputs = new InputState(); // state of user inputs
    public smoothSteer = true;
    public safeSteer = false;
    public inertia = 0; // will be = mass
    public wheelBase = 0; // set from axle to CG lengths
    public axleWeightRatioFront = 0; // % car weight on the front axle
    public axleWeightRatioRear = 0; // % car weight on the rear axle
    public config: any = {};
    public throttleInput: number = 0;
    public brakeInput: number = 0;
    public bodyMesh: THREE.Mesh;

    constructor();
    constructor(options: any);
    constructor(options?: any) {
        this.setConfig(options || {});

        var geometry = new THREE.BoxGeometry(20, 20, 20);
        var material = new THREE.MeshBasicMaterial({ color: 0xff0000, wireframe: true });

        this.bodyMesh = new THREE.Mesh(geometry, material);
    }

    /**
     *  Car setup params and magic constants.
     */
    private setConfig(options: any): void {
        this.config.gravity = options.gravity || 9.81;  // m/s^2
        this.config.mass = options.mass || 1200.0;  // kg
        this.config.inertiaScale = options.inertiaScale || 1.0;  // Multiply by mass for inertia
        this.config.halfWidth = options.halfWidth || 0.8; // Centre to side of chassis (metres)
        this.config.cgToFront = options.cgToFront || 2.0; // Centre of gravity to front of chassis (metres)
        this.config.cgToRear = options.cgToRear || 2.0;   // Centre of gravity to rear of chassis
        this.config.cgToFrontAxle = options.cgToFrontAxle || 1.25;  // Centre gravity to front axle
        this.config.cgToRearAxle = options.cgToRearAxle || 1.25;  // Centre gravity to rear axle
        this.config.cgHeight = options.cgHeight || 0.55;  // Centre gravity height
        this.config.wheelRadius = options.wheelRadius || 0.3;  // Includes tire (also represents height of axle)
        this.config.wheelWidth = options.wheelWidth || 0.2;  // Used for render only
        this.config.tireGrip = options.tireGrip || 2.0;  // How much grip tires have

        // % of grip available when wheel is locked
        this.config.lockGrip = (typeof options.lockGrip === 'number') ? GMath.clamp(options.lockGrip, 0.01, 1) : 0.7;
        this.config.engineForce = options.engineForce || 8000.0;
        this.config.brakeForce = options.brakeForce || 12000.0;
        this.config.eBrakeForce = options.eBrakeForce || this.config.brakeForce / 2.5;

        // How much weight is transferred during acceleration/braking
        this.config.weightTransfer = (typeof options.weightTransfer === 'number') ? options.weightTransfer : 0.2;
        this.config.maxSteer = options.maxSteer || 0.6;  // Maximum steering angle in radians
        this.config.cornerStiffnessFront = options.cornerStiffnessFront || 5.0;
        this.config.cornerStiffnessRear = options.cornerStiffnessRear || 5.2;

        // air resistance (* vel)
        this.config.airResist = (typeof options.airResist === 'number') ? options.airResist : 2.5;

        // rolling resistance force (* vel)
        this.config.rollResist = (typeof options.rollResist === 'number') ? options.rollResist : 8.0;
        this.inertia = this.config.mass * this.config.inertiaScale;
        this.wheelBase = this.config.cgToFrontAxle + this.config.cgToRearAxle;
        this.axleWeightRatioFront = this.config.cgToRearAxle / this.wheelBase; // % car weight on the front axle
        this.axleWeightRatioRear = this.config.cgToFrontAxle / this.wheelBase; // % car weight on the rear axle
    }

    public doPhysics(deltaTime: number): void {
        // Shorthand
        var cfg = this.config;

        // Pre-calc heading vector
        var sn = Math.sin(this.heading);
        var cs = Math.cos(this.heading);

        // Get velocity in local car coordinates
        this.velocityLocal.x = cs * this.velocity.x + sn * this.velocity.y;
        this.velocityLocal.y = cs * this.velocity.y - sn * this.velocity.x;

        // Weight on axles based on centre of gravity and weight shift due to forward/reverse acceleration
        var axleWeightFront = cfg.mass * (this.axleWeightRatioFront * cfg.gravity - cfg.weightTransfer *
            this.accelerationLocal.x * cfg.cgHeight / this.wheelBase);

        var axleWeightRear = cfg.mass * (this.axleWeightRatioRear * cfg.gravity + cfg.weightTransfer *
            this.accelerationLocal.x * cfg.cgHeight / this.wheelBase);

        // Resulting velocity of the wheels as result of the yaw rate of the car body.
        // v = yawrate * r where r is distance from axle to CG and yawRate (angular velocity) in rad/s.
        var yawSpeedFront = cfg.cgToFrontAxle * this.yawRate;
        var yawSpeedRear = -cfg.cgToRearAxle * this.yawRate;

        // Calculate slip angles for front and rear wheels (a.k.a. alpha)
        var slipAngleFront = Math.atan2(this.velocityLocal.y + yawSpeedFront, Math.abs(this.velocityLocal.x)) -
            GMath.sign(this.velocityLocal.x) * this.steerAngle;

        var slipAngleRear = Math.atan2(this.velocityLocal.y + yawSpeedRear, Math.abs(this.velocityLocal.x));

        var tireGripFront = cfg.tireGrip;

        // reduce rear grip when ebrake is on
        var tireGripRear = cfg.tireGrip * (1.0 - this.inputs.ebrake * (1.0 - cfg.lockGrip));

        var frictionForceFront_cy = GMath.clamp(-cfg.cornerStiffnessFront * slipAngleFront, -tireGripFront,
            tireGripFront) * axleWeightFront;

        var frictionForceRear_cy = GMath.clamp(-cfg.cornerStiffnessRear * slipAngleRear, -tireGripRear,
            tireGripRear) * axleWeightRear;

        //  Get amount of brake/throttle from our inputs
        var brake = Math.min(this.inputs.brake * cfg.brakeForce + this.inputs.ebrake * cfg.eBrakeForce, cfg.brakeForce);
        var throttle = this.inputs.throttle * cfg.engineForce;

        //  Resulting force in local car coordinates.
        //  This is implemented as a RWD car only.
        var tractionForce_cx = throttle - brake * GMath.sign(this.velocityLocal.x);
        var tractionForce_cy = 0;

        var dragForce_cx = -cfg.rollResist * this.velocityLocal.x - cfg.airResist * this.velocityLocal.x *
            Math.abs(this.velocityLocal.x);

        var dragForce_cy = -cfg.rollResist * this.velocityLocal.y - cfg.airResist * this.velocityLocal.y *
            Math.abs(this.velocityLocal.y);

        // total force in car coordinates
        var totalForce_cx = dragForce_cx + tractionForce_cx;
        var totalForce_cy = dragForce_cy + tractionForce_cy + Math.cos(this.steerAngle) * frictionForceFront_cy +
            frictionForceRear_cy;

        // acceleration along car axes
        this.accelerationLocal.x = totalForce_cx / cfg.mass;  // forward/reverse accel
        this.accelerationLocal.y = totalForce_cy / cfg.mass;  // sideways accel

        // acceleration in world coordinates
        this.acceleration.x = cs * this.accelerationLocal.x - sn * this.accelerationLocal.y;
        this.acceleration.y = sn * this.accelerationLocal.x + cs * this.accelerationLocal.y;

        // update velocity
        this.velocity.x += this.accelerationLocal.x * deltaTime;
        this.velocity.y += this.accelerationLocal.y * deltaTime;

        this.absoluteVelocity = this.velocity.length();

        // calculate rotational forces
        var angularTorque = (frictionForceFront_cy + tractionForce_cy) * cfg.cgToFrontAxle - frictionForceRear_cy *
            cfg.cgToRearAxle;

        //  Sim gets unstable at very slow speeds, so just stop the car
        if (Math.abs(this.absoluteVelocity) < 0.5 && !throttle) {
            this.velocity.x = this.velocity.y = this.absoluteVelocity = 0;
            angularTorque = this.yawRate = 0;
        }

        var angularAccel = angularTorque / this.inertia;

        this.yawRate += angularAccel * deltaTime;
        this.heading += this.yawRate * deltaTime;

        //  finally we can update position
        this.position.x += this.velocity.x * deltaTime;
        this.position.y += this.velocity.y * deltaTime;

        //  Display some data
        /*
        this.stats.clear();  // clear this every tick otherwise it'll fill up fast
        this.stats.add('speed', this.velocity_c.x * 3600 / 1000);  // km/h
        this.stats.add('accleration', this.accel_c.x);
        this.stats.add('yawRate', this.yawRate);
        this.stats.add('weightFront', axleWeightFront);
        this.stats.add('weightRear', axleWeightRear);
        this.stats.add('slipAngleFront', slipAngleFront);
        this.stats.add('slipAngleRear', slipAngleRear);
        this.stats.add('frictionFront', frictionForceFront_cy);
        this.stats.add('frictionRear', frictionForceRear_cy);
        */
    }

    /**
     * Smooth Steering
     * Apply maximum steering angle change velocity.
     */
    public applySmoothSteer(steerInput: number, deltaTime: number): number {
        var steer = 0;

        if (Math.abs(steerInput) > 0.001) {
            //  Move toward steering input
            steer = GMath.clamp(this.steerInput + steerInput * deltaTime * 2.0, -1.0, 1.0); // -inp.right, inp.left);
        } else {
            //  No steer input - move toward centre (0)
            if (this.steerInput > 0) {
                steer = Math.max(this.steerInput - deltaTime * 1.0, 0);
            } else if (this.steerInput < 0) {
                steer = Math.min(this.steerInput + deltaTime * 1.0, 0);
            }
        }

        return steer;
    }

    /**
     * Safe Steering
     * Limit the steering angle by the speed of the car.
     * Prevents oversteer at expense of more understeer.
     */
    public applySafeSteer(steerInput: number): number {
        var avel = Math.min(this.absoluteVelocity, 250.0);  // m/s
        var steer = steerInput * (1.0 - (avel / 280.0));
        return steer;
    }

    /**
     * @param deltaTime Delta Time in milliseconds
     */
    public update(deltaTime: number): void {
        var dt = deltaTime / 1000;  // delta T in seconds

        this.throttleInput = this.inputs.throttle;
        this.brakeInput = this.inputs.brake;

        var steerInput = this.inputs.left - this.inputs.right;

        //  Perform filtering on steering...
        if (this.smoothSteer) {
            this.steerInput = this.applySmoothSteer(steerInput, dt);
        } else {
            this.steerInput = steerInput;
        }

        if (this.safeSteer) {
            this.steerInput = this.applySafeSteer(this.steerInput);
        }

        //  Now set the actual steering angle
        this.steerAngle = this.steerInput * this.config.maxSteer;

        //
        //  Now that the inputs have been filtered and we have our throttle,
        //  brake and steering values, perform the car physics update...
        //
        this.doPhysics(dt);

        this.bodyMesh.position.set(this.position.x, this.position.y, 0);
        this.bodyMesh.rotateZ(this.heading);
    }
}