/// <reference path="./dsp.d.ts" />

console.log(hoge(0));

function calcCorr(signal:Float32Array, input:Float32Array):Float32Array {
  var fft = new FFT(input.length, 44100);
  fft.forward(signal);
  var sig_spectrum = new Float32Array(fft.spectrum);
  var sig_real = new Float32Array(fft.real);
  var sig_imag = new Float32Array(fft.imag);
  fft.forward(input);
  var spectrum = new Float32Array(fft.spectrum);
  var real = new Float32Array(fft.real);
  var imag = new Float32Array(fft.imag);
  var cross_real = Array.prototype.map.call(real, (_, i)=> sig_real[i] * real[i] / real.length );
  var cross_imag = Array.prototype.map.call(imag, (_, i)=>-sig_real[i] * imag[i] / imag.length );
  var inv_real = fft.inverse(cross_real, cross_imag);
  return inv_real;
}