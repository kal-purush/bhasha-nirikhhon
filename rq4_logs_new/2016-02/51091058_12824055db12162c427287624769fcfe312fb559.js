"use strict";
var Robot;
(function() {
    Robot = function(a) {
      this.parentState = new s(a);
      this.parentState.strafeDirection = 1;
    };
    var a = Robot.prototype;
    a.onIdle = function(a) {
      var b = a.robot;
      if (b.life < 30 && !b.parentId)
        while (1)

      var c = c * c;
      try {
        if (b.parentId == null && b.availableClones != 0) {
          b.clone();
          this.cloneState = new s(b);
          this.cloneState.position = null;
          this.cloneState.parentId = b.id;
          this.cloneState.id = null;
          this.cloneState.tracker = this.parentState.tracker;
          this.cloneState.strafeDirection = -1 * this.parentState.strafeDirection;
          this.cloneState.time = this.parentState.time;
          return;
        }
        var d = this.getState(b);
        d.update(b);
        var e = this.otherState(d);
        if (e.parentId == null && e.time != d.time) {
          d.time = Math.max(d.time, e.time);
          e.time = d.time;
        }
        this.next(b);
      } catch (f) {

      }
    };
    a.onRobotCollision = function(a) {
      var c = a.robot;
      var d = this.getState(c);
      d.update(c);
      d.tracker.push(a.collidedRobot, d);
      var e = b.FromBearing(c.angle, a.bearing);
      var f = d.direction.dotProduct(e);
      if (f > 0.3) c.move(1, -1);
      else if (f < -0.3) c.move(1, 1);
      else c.turn(1);
    };
    a.onScannedRobot = function(a) {
      var b = a.robot;
      b.disappear();
      var c = this.getState(b);
      c.update(b);
      c.tracker.push(a.scannedRobot, c);
      this.next(b);
    };
    a.onWallCollision = function(a) {
      var b = a.robot;
      var c = this.getState(b);
      c.update(b);
      this.next(b);
    };
    a.onHitByBullet = function(a) {
      var c = a.robot;
      c.disappear();
      var e = this.getState(c);
      e.update(c);
      var f = b.FromBearing(c.angle, a.bearing);
      var g = e.arena.marginRect.getDistToIntersection(new d(e.position, f), true);
      var h = e.position.add(f.scale(0.5 * g));
      var i = b.FromAngle(0).signedAngleTo(f.flip());
      var j = new HitByBullet(null, h, null, i, null, null);
      e.tracker.push(j, e);
      this.next(c);
    };
    a.next = function(a) {
      try {
        var b = this.getState(a);
        if (!b.isInitialized) this.initialize(a, b);
        if (this.updateCannon(a, b)) {
          if (b.tracker.isHunting(b) && b.arcAndDirection == null) this.setToSeekMode(a, b);
          return;
        }
        this.updateStrafeDirection(a, b);
        if (b.arcAndDirection != null && !b.arcAndDirection.isPastEnd(b, b.arcAndDirection.targetPoint)) b.arcAndDirection.update(b, b.arcAndDirection.targetPoint);
        if (b.tracker.isHunting(b)) this.huntNext(a, b);
        else this.scanNext(a, b);
      } catch (c) {}
    };
    a.scanNext = function(a, b) {
      try {
        if (b.arcAndDirection != null && !b.arcAndDirection.isScanArc()) {
          b.strafeDirection = b.arcAndDirection.direction;
          b.arcAndDirection = null;
        }
        if (b.strafeDirection == 0) b.strafeDirection = a.cannonRelativeAngle < 90 ? -1 : 1;
        this.moveToArc(a, b, b.arcAndDirection);
      } catch (c) {}
    };
    a.huntNext = function(a, b) {
      try {
        var c = b.tracker.lastTrackingScans(null, b).timeSinceLast(b);
        var d = b.position;
        var e = b.tracker.isTracking(b);
        if (a.gunCoolDownTime == 0) {
          if (!e && this.canFire(b)) {
            a.fire();
            this.setToSeekMode(a, b);
            return;
          }
          var f = b.tracker.getAttackPoint(a, b, null);
          var g = d.distanceTo(f);
          var h = d.add(b.cannonDirection.scale(g));
          var i = h.distanceTo(f);
          if (i < (0.6 * b.arena.radius) && this.canFire(b)) {
            a.fire();
            if (b.arcAndDirection == null || b.arcAndDirection.isAttacArc()) this.setToSeekMode(a, b);
            return;
          }
        }
        if (e && c < 25 || (b.arcAndDirection != null && b.arcAndDirection.isAttacArc())) {
          if (b.arcAndDirection == null || !b.arcAndDirection.isAttacArc()) {
            var f = b.tracker.getAttackPoint(a, b, null);
            b.arcAndDirection = new q(b, f, a.gunCoolDownTime, q.attackArcEnum());
          } else if (b.arcAndDirection.isAttacArc() && b.arcAndDirection.isPastEnd(b, b.arcAndDirection.targetPoint)) {
            var f = b.tracker.getAttackPoint(a, b, null);
            b.arcAndDirection.update(b, f);
          }
        } else this.setToSeekMode(a, b);
        this.moveToArc(a, b, b.arcAndDirection);
      } catch (j) {}
    };
    a.setToSeekMode = function(a, b) {
      try {
        var c = b.tracker.lastTrackingScans(null, b).timeSinceLast(b);
        if (b.arcAndDirection == null || !b.arcAndDirection.isSeekArc()) {
          var d = b.tracker.nextSeekPoint(a, b);
          b.arcAndDirection = new q(b, d.targetPoint, d.targetTimeOffset, d.arcEnum);
        } else if (b.arcAndDirection.isSeekArc()) {
          if (b.arcAndDirection.isPastEnd(b, b.arcAndDirection.targetPoint) && c > 55) {
            var d = b.tracker.nextSeekPoint(a, b);
            b.arcAndDirection = new q(b, d.targetPoint, d.targetTimeOffset, d.arcEnum);
          }
          if (b.arcAndDirection.isPastEnd(b, b.arcAndDirection.targetPoint) && c <= 50) b.arcAndDirection.update(b, b.arcAndDirection.targetPoint);
        }
      } catch (e) {}
    };
    a.updateCannon = function(a, b) {
      try {
        var c = b.getScanningCannonAngle(a);
        var e = b.angleDiff(c, a.cannonRelativeAngle);
        if (b.tracker.isHunting(b)) {
          var f = b.position;
          if (b.arcAndDirection == null) return false;
          if (b.strafeDirection == 0) b.strafeDirection = -1 * b.sign(this.cannonAngleDiff(a, b, e));
          if (this.isCrashing(a, b, 10)) {
            var g = b.arcAndDirection != null && b.arcAndDirection.arc.radius > 1 ? b.arcAndDirection.getMoveSign(b) : 0;
            var h = b.direction.scale(g);
            var i = new d(f, h);
            var j = b.arena.marginRect.getInterSectingLines(i, true);
            var k = j.length > 0 ? Math.abs(j[0].direction().signedAngleTo(h)) : 0;
            var l = this.cannonAngleDiff(a, b, e);
            if (Math.abs(l) < 1 && (k > 45 || k < 135)) return false;
            a.rotateCannon(b.sign(l) * b.strafeDirection);
            return true;
          }
          if (b.tracker.timeSinceLast(b, null) < 40 && a.gunCoolDownTime > 15 && b.time % 5 == 0) {
            var l = this.cannonAngleDiff(a, b, e);
            a.rotateCannon(b.sign(l) * b.strafeDirection);
            return true;
          }
        } else if (Math.abs(e) > 1) {
          a.rotateCannon(this.parentState.getRotationDirection(a.cannonRelativeAngle, c));
          return true;
        }
        return false;
      } catch (m) {}
    };
    a.updateStrafeDirection = function(a, b) {
      try {
        var c = b.position;
        if (this.isCrashingWithOpponent(a, b, 1)) b.strafeDirection *= -1;
        var d = b.arena.marginRect.minDistanceToPoint(c) < (b.arena.radius + 5) && this.isCrashing(a, b, 5);
        if (d) {
          var e = this.distToIdeal(a, b);
          if (e < 0) {
            b.strafeDirection *= -1;
            return;
          }
        }
        var f = this.isCrashingWithFriend(a, b, 5);
        var g = this.isShadowedByOther(a, b);
        var h = this.otherState(b);
        var i = this.isShadowedByOther(a, h);
        if (f || g || i) {
          var e = this.distToIdeal(a, b);
          var j = this.distToIdeal(a, h);
          var k = this.strafeArc(a, b);
          var l = k.centerPoint;
          var m = l.vectorTo(c);
          var n = l.vectorTo(h.position);
          var o = m.signedAngleTo(n);
          if (Math.abs(e) <= 110 && b.strafeDirection != (-1 * b.sign(o))) b.strafeDirection = -1 * b.sign(o);
          if (Math.max(Math.abs(e), Math.abs(j)) > 100 && Math.abs(e) > Math.abs(j) && b.strafeDirection != (-1 * b.sign(o))) b.strafeDirection = -1 * b.sign(o);
          return;
        }
        var p = this.distToIdeal(a, b);
        if (p < -100) b.strafeDirection *= -1;
      } catch (q) {}
    };
    a.cannonAngleDiff = function(a, b, c) {
      var e = b.position;
      var f = this.strafeArc(a, b);
      var g = this.idealPoint(a, b);
      var h = f.centerPoint;
      var i = h.distanceTo(e);
      var j = b.arena.marginRect.getDistToIntersection(new d(h, h.vectorTo(e)), true);
      var k = Math.min(j, h.distanceTo(g));
      var l = k - i;
      var m = Math.atan2(l, 100) * 180 / Math.PI;
      var n = b.strafeDirection * c;
      var o = m - n;
      return o;
    };
    a.isCrashing = function(a, b, c) {
      var d = b.arcAndDirection != null && b.arcAndDirection.arc.radius > 1 ? b.arcAndDirection.getMoveSign(b) : 0;
      if (d == 0) return false;
      var e = b.position;
      var f = b.direction.scale(d);
      var g = e.add(f.scale(c));
      return !g.isWithin(b.arena.marginRect, 0);
    };
    a.isCrashingWithFriend = function(a, b, c) {
      var d = this.otherState(b);
      if (b.arcAndDirection == null || b.arcAndDirection.arc.radius < 1 || d == null || d.position == null || Math.abs(b.time - d.time) > 10) return false;
      var e = b.position;
      var f = d.position;
      if (e.distanceTo(f) > (2 * b.arena.radius + c)) return false;
      var g = b.direction.scale(b.arcAndDirection.getMoveSign(b));
      var h = e.add(g.scale(c));
      var i = h.distanceTo(d.position);
      if (i < (2 * b.arena.radius)) return true;
      return false;
    };
    a.isCrashingWithOpponent = function(a, b, c) {
      if (b.arcAndDirection == null || b.arcAndDirection.arc.radius < 1 || b.tracker.timeSinceLast(b, null) > 50) return false;
      var d = b.position;
      var e = b.tracker.last(null).robot.position;
      if (d.distanceTo(e) > (2 * b.arena.radius + c)) return false;
      var f = b.direction.scale(b.arcAndDirection.getMoveSign(b));
      var g = d.add(f.scale(c));
      var h = g.distanceTo(e);
      if (h < (2 * b.arena.radius)) return true;
      return false;
    };
    a.isShadowedByOther = function(a, b) {
      if (b == null || b.position == null) return false;
      var c = this.otherState(b);
      if (b.arcAndDirection == null || c == null || c.position == null || a.gunCoolDownTime > 25 && b.tracker.timeSinceLast(b, null) < 25 || Math.abs(b.time - c.time) > 10) return false;
      var e = b.position;
      var f = c.position;
      var g = b.cannonDirection;
      var h = new d(e, g);
      var i = h.closestPoint(f);
      if (!i.isOnRay(h)) return false;
      var j = f.distanceTo(i);
      return j < (3 * b.arena.radius);
    };
    a.distToIdeal = function(a, b) {
      if (!b.tracker.isHunting(b)) return 0;
      var c = b.position;
      var d = this.strafeArc(a, b);
      var e = d.centerPoint;
      var f = this.idealPoint(a, b);
      var g = e.vectorTo(f);
      var h = e.vectorTo(c);
      var i = h.signedAngleTo(g);
      var j = b.sign(i) * b.strafeDirection * d.arcLengthFromAngle(i);
      return j;
    };
    a.moveToArc = function(a, b, c) {
      if (c == null) {
        if (b.strafeDirection == 0) b.strafeDirection = 1;
        a.turn(b.strafeDirection);
        return;
      }
      var d = c.arc;
      if (c.direction == 0) c.direction = 1;
      if (d.radius < 1e-3) {
        a.turn(c.direction);
        return;
      }
      var e = d.centerPoint;
      var f = b.position;
      var g = b.direction.scale(c.getMoveSign(b));
      var h = f.add(g);
      var i = e.distanceTo(h);
      if (i > d.radius) {
        a.turn(c.direction);
        return;
      }
      a.move(1, c.getMoveSign(b));
    };
    a.driveWithin = function(a, b) {
      var c = b.position;
      var d = b.arena.borderRect.minDistanceToPoint(c);
      var e = b.arena.borderRect.minDistanceToPoint(c.add(b.direction));
      if (Math.abs(d - e) > 0.2)
        if (e > d) a.move(1, 1);
        else a.move(1, -1);
      else a.turn(1);
      b.arcAndDirection = null;
    };
    a.initialize = function(a, b) {
      try {
        var c = b.position;
        var d = b.cannonDirection;
        var e = this.otherState(b);
        if (e == null) {
          var f = c.vectorTo(b.arena.centerPoint());
          var g = d.signedAngleTo(f);
          if (g > 0) {
            a.rotateCannon(1);
            b.strafeDirection = 1;
          } else {
            a.rotateCannon(-1);
            b.strafeDirection = -1;
          }
        } else {
          if (e.position == null) return;
          var h = e.position;
          var i = c.vectorTo(h);
          var j = d.signedAngleTo(i);
          a.rotateCannon(-1 * b.sign(j));
          b.strafeDirection = -1 * b.sign(j);
        }
        b.isInitialized = true;
      } catch (k) {}
    };
    a.canFire = function(a) {
      var b = this.otherState(a);
      if (b == null) return true;
      if (Math.abs(a.time - b.time) > 10) return true;
      var c = a.position;
      var e = new d(c, a.cannonDirection);
      var f = b.position;
      var g = e.closestPoint(f);
      if (!g.isOnRay(e)) return true;
      return g.distanceTo(f) > (1.5 * a.arena.radius);
    };
    a.getState = function(a) {
      var b = (a.parentId == null) ? this.parentState : this.cloneState;
      return b;
    };
    a.otherState = function(a) {
      var b = a.parentId == null ? this.cloneState : this.parentState;
      return b;
    };
    a.strafeArc = function(a, b) {
      var c = b.position;
      var d = b.tracker.isTracking(b);
      var e = b.tracker.isHunting(b);
      if (!d && !e) return new f(b.arena.centerPoint(), b.arena.centerPoint().distanceTo(c));
      if (e && !d) {
        var g = b.tracker.last(null).robot.position;
        return new f(g, g.distanceTo(c));
      }
      var h = b.tracker.predict(b, 0, null);
      return new f(h, c.distanceTo(h));
    };
    a.idealPoint = function(a, b) {
        var c = b.position;
        if (!b.tracker.isHunting(b)) return c;
        var d = this.strafeArc(a, b);
        var f = b.tracker.predict(b, 0, null);
        var g = b.tracker.lastTrackingScans(null, b);
        var h = g.last().robot.direction;
        var i = d.centerPoint;
        var j = i.vectorTo(c);
        if (b.tracker.isWallHugger(null, b)) return b.arena.centerPoint();
        var k = d.getIntersectionPoints(new e(f.add(h.scale(-2 * d.radius)), f.add(h.scale(2 * d.radius))));
        if (k.length < 2) return b.arena.centerPoint();
        var l = this.createIdealPoint(b, i, k[0]);
        var m = i.vectorTo(l);
        var n = j.signedAngleTo(m);
        var o = Math.acos(Math.abs(h.normalize().dotProduct(m.normalize()))) * 180 / Math.PI;
        if (Math.abs(n) < 90 && Math.abs(o) < 10) return l;
        var p = this.createIdealPoint(b, i, k[1]);
        var q = i.vectorTo(p);
        var r = j.signedAngleTo(q);
        var s = Math.acos(Math.abs(h.normalize().dotProduct(q.normalize()))) * 180 / Math.PI;
        if (Math.abs(r) < 90 && Math.abs(s) < 10) return p;
        if (Math.abs(n)