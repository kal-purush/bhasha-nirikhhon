import client from "prom-client";

const histograms = {} as any;

export function initHistogram(name: string) {
  const histogram = new client.Histogram({
    name,
    help: name,
    buckets: [0.1, 2, 5, 10],
  });
  histograms[name] = histogram;
}
/**
 * Register a duration for some histogram
 */
export function registerDuration(name: string, durationMs: number): void {
  histograms[name].observe(durationMs / 1000);
}

export async function getMetrics() {
  const res = {} as any;
  const metrics = (await client.register.getMetricsAsJSON()) as any;
  metrics
    .filter((m: any) => m.type === "histogram")
    .forEach((histogram: any) => {
      const performanceMetrics: any = {};
      histogram?.values.forEach((bucket: any) => {
        const label = "<= " + bucket.labels.le;
        performanceMetrics[label] = bucket.value;
      });

      res[histogram.name] = performanceMetrics;
    });

  return res;
}