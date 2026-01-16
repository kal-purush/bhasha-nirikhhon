import { Meta } from "@storybook/html";
import Snap from "snapsvg";
import { addCoords, Coords } from "../coords";
import { Patterns } from "../patterns";
import { gridBuilder } from "../patterns/grids";
import { Producers } from "../producers";
import { Polygons } from "../shapes/regular-polygons";
import { triangle } from "../shapes/triangles";
import { makeStoryWithSnap } from "./stories-util";

export default {
  title: "Patternator/Examples",
  args: {
    drawDebugHelp: false,
  },
} as Meta;

// ======================== fairly weak examples ... too noisy ?

export const DiagonalOfLargeDots = makeStoryWithSnap((snap, args) => {
  Patterns.startAt({ x: 50, y: 50 })
    .transposeBy({ x: 50, y: 20 })
    .times(15)
    .do(circle(snap));
});

export const RandomlyLocatedCurveOfLargeDots = makeStoryWithSnap(
  (snap, args) => {
    Patterns.startAt({ x: Producers.rnd(200), y: Producers.rnd(200) })
      .transposeBy({ x: Producers.inc(3), y: Producers.dec(10) })
      .times(7)
      .do(circle(snap));
  }
);

export const TODONicerSyntaxForProducerBasedTransformers = makeStoryWithSnap(
  (s, args) => {
    // TODO: do the following with a nicer syntax and the producers:
    [0, 120, 240].map((a) => {
      s.polygon([40, 30, 60, 60, 20, 60])
        .attr({ stroke: "#3e7ed2" })
        .transform(`r${a},60,60`);
      // transform() supports 2 notations; r180 seems to be snap-specific and is always relative, whereas rotate() is the same as css and isn't relative?
    });
  }
);

// ======================== now this is getting more interesting

export const GridWithNativePolygons = makeStoryWithSnap((s, args) => {
  gridBuilder()
    .grid(8, 8, 40, 40)
    .do((c) => {
      s.polygon([c.x + 40, c.y + 30, c.x + 50, c.y + 60, c.x + 30, c.y + 60])
        .attr({ stroke: "#4d9d9f" })
        .clone()
        .transform("r180 t-20 3");
    });
});
export const GridOfTriangles = makeStoryWithSnap((s, args) => {
  // TODO: nicer syntax, don't muck about with .transform and injection of coordinates
  // instead the "repeater" could have methods to rotate, etc and optionally a more freeform delegate to transform()
  gridBuilder()
    .grid(8, 8, 60, 87)
    .do((c) => {
      s.path(triangle(40, 80, 80).pathSpec)
        .attr({ stroke: "#8d3d4d" })
        .transform(`T${c.x} ${c.y}`)
        .clone()
        .transform(`r180 T${c.x - 30} ${c.y}`);
    });
});

export const QBertCubes = makeStoryWithSnap((s, args) => {
  // TODO: INNER OUTER STROKES --> MOVE THIS TO UTILITY
  // TODO: ROMBUS shape --> move to class
  // https://alexwlchan.net/2021/03/inner-outer-strokes-svg/
  const tm = s.polygon([0, 20, 30, 0, 60, 20, 30, 40]).toDefs();
  const bl = s.polygon([0, 20, 30, 40, 30, 60, 0, 40]).toDefs();
  const br = s.polygon([30, 40, 60, 20, 60, 40, 30, 60]).toDefs();
  const thingo = s
    .group(
      (s.use(tm) as Snap.Element)
        .attr({ clipPath: tm, strokeWidth: 6, stroke: "#777" })
        .toDefs(),
      (s.use(bl) as Snap.Element)
        .attr({ clipPath: bl, strokeWidth: 6, stroke: "#444" })
        .toDefs(),
      (s.use(br) as Snap.Element)
        .attr({ clipPath: br, strokeWidth: 6, stroke: "#555" })
        .toDefs()
    )
    .toDefs();
  const placeThingo = (c: Coords) =>
    (s.use(thingo) as Snap.Element).transform(`T${c.x} ${c.y}`);

  const viewBox = s.attr("viewBox");
  const oppositeCorner = { x: viewBox.width, y: viewBox.height };

  gridBuilder({ x: 0, y: -40 })
    .gridUntil(oppositeCorner, 60, 80)
    .do(placeThingo);
  gridBuilder({ x: -30, y: 0 })
    .gridUntil(oppositeCorner, 60, 80)
    .do(placeThingo);
});

export const GridOnHex = makeStoryWithSnap((s, args) => {
  // TODO this is completely inefficient
  const polygonAround = (sides: number) => (center: Coords) =>
    Polygons.byOuterRadius(sides, 100).vertices.map((c) =>
      // See Producers.transpose
      addCoords(c, center)
    );

  polygonAround(6)({ x: 410, y: 370 })
    .flatMap(polygonAround(6))
    .flatMap(polygonAround(6))
    .flatMap(polygonAround(6))
    .map(sqr(s));

  // center point:
  s.rect(408, 368, 4, 4).attr({ fill: "green", stroke: "none" });

  // top left corner?
  s.rect(10, 10, 820, 730).attr({ fill: "none", stroke: "green" });
});

export const LinesPatternsAreWack = makeStoryWithSnap((s, args) => {
  // TODO these could actually be nice if it was more controlled
  const MAX_X = 6000;
  const MAX_Y = 6000;

  const lines = (px: number, py: number, dx: number = px) => {
    // Multiply by MAX_* == magic number to ensure we cross the boundary of the canvas -- which is really ensuring nothing if we don't do the math here
    // whatever px meant, we augment it and create a bigger angle... i don't think it was meant to do be used this way...
    px = px * 2000;
    py = py * 2000;
    for (var x = -px; -MAX_X < x && x < MAX_X; x += dx) {
      s.line(x, 0, x + px, py);
    }
  };

  // lines(60, 80, 60);
  // lines(3, 4, 60);
  // lines(-3, 4, -60);
  // lines(-7, 3, -60);
  // lines(-6, 3, -60);
  // lines(-5, 3, -60);
  // lines(-4, 3, -60);
  // lines(-3, 3, -60);
  // lines(-2, 3, -120);
  // lines(-1, 3, -60);
  // lines(0, 3, 60);
  // lines(0.5, 3, 60);
  lines(1, 3, 60);
  lines(2, 3, 60);
  // lines(2, 3, 120);
  // lines(2, 4, 120);

  // TODO merge transposeBy() and times()?
  // not sure there's many interesting use-cases for a set nr of repetitions, instead
  // have automated repetitions that take the pattern out of screen (or better yet make it infinitely repeating over a cylinder)

  Patterns.startAt({
    x: -500,
    y: 0,
  })
    .transposeBy({ x: 60 })
    .times(60)
    .do((c) =>
      s.line(c.x, c.y, c.x + 100, c.y + 2000).attr({ stroke: "#8d2" })
    );

  // lines(4, 3 , 60);
  // lines(5, 3 , 60);
  // no output, too flat?
  //   lines(6, 3 , 60);
  // lines(7, 3 , 60);
  // lines(8, 3 , 60);
  // lines(9, 3 , 60);
  // lines(10, 3 , 60);
});

// ======================== utilities
const circle = (snap: Snap.Paper) => (coords: Coords) => {
  snap.circle(coords.x, coords.y, 5).attr({
    fill: "#300",
    stroke: "#933",
    strokeWidth: 2,
  });
};

const sqr = (snap: Snap.Paper) => (coords: Coords) =>
  snap
    .rect(coords.x - 2, coords.y - 2, 4, 4)
    .attr({ fill: "#444", stroke: "none" });