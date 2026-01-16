import { Deserializable } from './deserializable.model';

// Para que funcione la carga de fotos es necesario implementar una función de serialización y deserialización específica de objetos
export class Picture implements Deserializable {
    Buffer: string;
    contentType: string;

    // Método para deserializar una imagen
    deserialize(input: any) {
        Object.assign(this, input);
        return this;
    }

    // Método para serializar una imagen
    toJSON() {
        return {
            Buffer: this.Buffer,
            contentType: this.contentType
        };
    }
}