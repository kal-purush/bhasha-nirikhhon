namespace interfaces1sample {
  "use strict";

  interface MyConf {
    host: string;
    port: number;
    path?: string;
  }

  let conf: MyConf = {
    host: "localhost",
    port: 3000,

    somethingElse: true
  };
}


namespace interfaces2sample {
  "use strict";

  interface Sample {
    name: string;
    age?: number;
  }

  class SampleHandling {
    log(sample: Sample) {
      console.log(`Sample with name ${sample.name}`);
    }

    logArray(samples: Sample[]) {
      for (let sample of samples) {
        this.log(sample);
      }
    }
  }

  let instance = new SampleHandling();

  let samples = [{name: "first"}, {name: "second"}];
  instance.logArray(samples);
}


namespace interfaces4sample {
  "use strict";

  interface Measurement {
    id: number;
    key: string;
    value: number;
  }

  interface MeasurementsLoader {
    loadData: () => Measurement[];
  }

  // mistype some property to see compile errors
  class SampleDataLoader implements MeasurementsLoader {
    loadData = () => [
      {id: 1, key: "sensor1", value: 2.1},
      {id: 2, key: "sensor1", value: 2.3},
      {id: 3, key: "sensor1", value: 2.2},
    ];
  }

  let loader: MeasurementsLoader = new SampleDataLoader();
  let measurements = loader.loadData();

  for (let m of measurements) {
    console.log(`Measurement ${m.id} with key ${m.key} has value ${m.value}`);
  }
}