using UnityEngine;

public class WavePhysicsController : MonoBehaviour {
  public WaveInput input;
  public Rigidbody rigidbody;

  public float drag;
  public float perpendicularDrag;

  float[] speed;
  float angle;

  void FixedUpdate() {
    if (speed == null || speed.Length == 0) {
      speed = new float[2] {0, 0};
    }

    var tSquared = Mathf.Pow(Time.fixedDeltaTime, 2);
    var accelSquared = input.accel * tSquared * 10;

    // var lastSpeedMagnitude = Mathf.Sqrt(speed[0] * speed[0] + speed[1] * speed[1]);
    var lastSpeedMagnitude = rigidbody.velocity.magnitude;

    // Turn the character
    // angle += input.direction * (
    //   lastSpeedMagnitude * tSquared * 2 +
    //   input.accel * tSquared * 5
    // );
    // rigidbody.angularVelocity = new Vector3(
    //   rigidbody.angularVelocity.x,
    //   rigidbody.angularVelocity.y * 0.5f + input.direction * (
    //     lastSpeedMagnitude * tSquared * 2 +
    //     input.accel * tSquared * 5
    //   ) * 0.5f,
    //   rigidbody.angularVelocity.z
    // );
    rigidbody.AddTorque(Vector3.down * rigidbody.angularVelocity.y * Time.fixedDeltaTime * 5, ForceMode.Impulse);
    rigidbody.AddTorque(new Vector3(
      // -input.direction * (
      //   lastSpeedMagnitude * tSquared * 2 +
      //   input.accel * tSquared * 5
      // ),
      0,
      -input.direction * (
        Mathf.Clamp(lastSpeedMagnitude * tSquared * 8, -0.5f, 0.5f) +
        input.accel * tSquared * 80
      ),
      // -input.direction * (
      //   lastSpeedMagnitude * tSquared * 2 +
      //   input.accel * tSquared * 5
      // )
      0
    ), ForceMode.Impulse);
    // Debug.Log(rigidbody.angularVelocity);

    // Apply acceleration to speed of character
    rigidbody.AddForce(input.accel * 100 * transform.forward, ForceMode.Acceleration);
    // rigidbody.AddForce(new Vector3(
    //   input.accel * 10 * 100 * -Mathf.Sin(angle),
    //   0,
    //   input.accel * 10 * 100 * Mathf.Cos(angle)
    // ), ForceMode.Acceleration);

    var speedAngle = Vector2.Angle(
      new Vector2(rigidbody.velocity[0], rigidbody.velocity[2]),
      new Vector2(Mathf.Cos(angle), Mathf.Sin(angle))
    );

    // Apply drag to speed
    // rigidbody.AddForce(rigidbody.velocity * -drag, ForceMode.VelocityChange);

    // Project speed against angle so that speed not in the facing direction of
    // the character is dragged off.
    // var speedMag = Mathf.Sqrt(speed[0] * speed[0] + speed[1] * speed[1]);
    var speedMag = rigidbody.velocity.magnitude;
    // Project: |a| * ((a * b) / (|a| * |b|)) * b / |b|
    // We can drop |a| and |b| parts of this formula. |b| causes its always 1.
    // |a| because without |b| we can see they'll become |a| / |a|.
    // var component = speed[0] * Mathf.Cos(angle) + speed[1] * Mathf.Sin(angle);
    var component = rigidbody.velocity.x * transform.forward.x + rigidbody.velocity.z * transform.forward.z;
    // speed[0] = speed[0] * (1 - perpendicularDrag) +
    //   component * Mathf.Cos(angle) * perpendicularDrag;
    // speed[1] = speed[1] * (1 - perpendicularDrag) +
    //   component * Mathf.Sin(angle) * perpendicularDrag;
    rigidbody.AddForce(rigidbody.velocity * -perpendicularDrag, ForceMode.VelocityChange);
    rigidbody.AddForce(transform.forward * component * perpendicularDrag, ForceMode.VelocityChange);

    // transform.position += new Vector3(-speed[1], 0, speed[0]) * Time.fixedDeltaTime;
    // transform.eulerAngles = new Vector3(0, -angle / Mathf.PI * 180, 0);
  }
}