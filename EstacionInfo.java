
// clase que representa la información de una estación de riego 
// mientras WM_Central está en ejecución
public class EstacionInfo {

    public static final String DISPONIBLE = "DISPONIBLE";
    public static final String REGANDO = "REGANDO";
    public static final String FUGA = "FUGA";
    public static final String FUERA_SERVICIO = "FUERA_SERVICIO";
    public static final String DESCONECTADA = "DESCONECTADA";

    private String idEstacion;
    private String ubicacion;
    private String estado;

    // datos que solo existen mientras la estación está regando
    private double caudalActual;        // litros/minuto
    private double volumenAcumulado;    // litros
    private String idOperarioRiego;     // quien activó el riego actual

    public EstacionInfo(String idEstacion, String ubicacion, String estado) {
        this.idEstacion = idEstacion;
        this.ubicacion = ubicacion;
        this.estado = estado;
        this.caudalActual = 0.0;
        this.volumenAcumulado = 0.0;
        this.idOperarioRiego = null;
    }

    // getters y setters
    public String getIdEstacion() { return idEstacion; }
    public String getUbicacion() { return ubicacion; }
    public String getEstado() { return estado; }
    public double getCaudalActual() { return caudalActual; }
    public double getVolumenAcumulado() { return volumenAcumulado; }
    public String getIdOperarioRiego() { return idOperarioRiego; }

    public void setUbicacion(String p_ubicacion) { this.ubicacion = p_ubicacion; }
    public void setEstado(String p_estado) { this.estado = p_estado; }
    public void setCaudalActual(double p_caudal) { this.caudalActual = p_caudal; }
    public void setVolumenAcumulado(double p_volumen) { this.volumenAcumulado = p_volumen; }
    public void setIdOperarioRiego(String p_idOperario) { this.idOperarioRiego = p_idOperario; }
    
    public String toString() {
        return idEstacion + " [" + ubicacion + "] estado =" + estado +
                ", caudal = " + caudalActual + "L/min, vol = " + volumenAcumulado
                + "L";
    }
}