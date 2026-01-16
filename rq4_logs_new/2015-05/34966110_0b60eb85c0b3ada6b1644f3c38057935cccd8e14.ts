/// <reference path="Packer.ts" />
/// <reference path="UnPacker.ts" />

module BinaryPackTs {

    class BinaryPack {
        UnPack(data: Int8Array): any {
            var unpacker = new UnPacker(data);
            return unpacker.UnPack();
        }

        Pack(data: any): Int8Array {
            var packer = new Packer();
            packer.Pack(data);
            var buffer = packer.GetBuffer();
            return buffer;
        }
    }
}