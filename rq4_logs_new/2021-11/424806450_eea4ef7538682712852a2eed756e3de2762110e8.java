public class Autos {
   private String Marca,Color,Matricula;
   private int Modelo;
  private boolean Disponible;

   public Autos(){
    this.Marca = "Volkswagen";
    this.Color = "morado";
    this.Matricula = "VLm21";
    this.Modelo = 2021;
    this.Disponible = true;
}
 
    public String getMarca() {
        return Marca;
    }
 
    public void setMarca(String Marca) {
        this.Marca = Marca;
    }
 
    public String getColor() {
        return Color;
    }
 
    public void setColor(String Color) {
         this.Color = Color;
    }
 
    public String getMatricula() {
        return Matricula;
    }
 
    public void setMatricula(String Matricula) {
        this.Matricula = Matricula;
    }
 
    public int getModelo() {
        return Modelo;
    }
 
    public void setModelo(int Modelo) {
        this.Modelo = Modelo;
    }
 
    public boolean getDisponible() {
        return Disponible;
    }
 
    public void setDisponible(boolean Disponible) {
        this.Disponible = Disponible;
    }

}