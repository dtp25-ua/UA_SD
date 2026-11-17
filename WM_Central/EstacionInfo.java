
// clase que representa la información de una estación de riego 
// mientras WM_Central está en ejecución
public class EstacionInfo {

    public static final String DISPONIBLE = "DISPONIBLE";
    public static final String REGANDO = "REGANDO";
    public static final String FUGA = "FUGA";
    public static final String FUERA_SERVICIO = "FUERA_SERVICIO";
    public static final String DESCONECTADA = "DESCONECTADA";

    private String idEstacion;

    // campos escritos por los hilos de las conexiones 
    // y los lee el del panel, por eso 'volatile'
    private volatile String ubicacion;
    private volatile String estado;
    private volatile boolean bloqueada;

    // para que Central no acepte dos riegos de una ws a la vez
    private volatile boolean riegoPendiente = false;

    // datos que solo existen mientras la estación está regando
    private volatile double caudalActual;        // litros/minuto
    private volatile double volumenAcumulado;    // litros
    private volatile String idOperarioRiego;     // quien activó el riego actual

    // hilo que atiende ahora mismo a esta estación
    private volatile Thread conexionActual;

    public EstacionInfo(String p_idEstacion, String p_ubicacion, String p_estado) {
        this.idEstacion = p_idEstacion;
        this.ubicacion = p_ubicacion;
        this.estado = p_estado;
        this.bloqueada = false;
        this.caudalActual = 0.0;
        this.volumenAcumulado = 0.0;
        this.idOperarioRiego = null;
        this.conexionActual = null;
    }

    // estado que se muestra en el panel de control, que puede ser distinto del real
    public String getEstadoVisible()
    {
        if (estado.equals(DESCONECTADA) || estado.equals(FUGA))
            return estado;
        if (bloqueada)
            return FUERA_SERVICIO;
        else
            return estado;
    }

    public boolean estaDisponible()
    {
        return getEstadoVisible().equals(DISPONIBLE) && !riegoPendiente;
    }

    // getters y setters
    public String getIdEstacion() { return idEstacion; }
    public String getUbicacion() { return ubicacion; }
    public String getEstado() { return estado; }
    public double getCaudalActual() { return caudalActual; }
    public double getVolumenAcumulado() { return volumenAcumulado; }
    public String getIdOperarioRiego() { return idOperarioRiego; }
    public boolean isBloqueada() { return bloqueada; }
    public Thread getConexionActual() { return conexionActual; }

    public void setUbicacion(String p_ubicacion) { this.ubicacion = p_ubicacion; }
    public synchronized void setEstado(String p_estado) { this.estado = p_estado; }
    public void setCaudalActual(double p_caudal) { this.caudalActual = p_caudal; }
    public void setVolumenAcumulado(double p_volumen) { this.volumenAcumulado = p_volumen; }
    public void setIdOperarioRiego(String p_idOperario) { this.idOperarioRiego = p_idOperario; }
    public void setBloqueada(boolean p_bloqueada) { this.bloqueada = p_bloqueada; }
    public void setConexionActual(Thread p_conexion) { this.conexionActual = p_conexion; }
    
    public String toString() {
        return idEstacion + " [" + ubicacion + "] estado =" + getEstadoVisible() +
                ", caudal = " + caudalActual + "L/min, vol = " + volumenAcumulado
                + "L";
    }

    // reserva la estacion para un riego
	public synchronized boolean reservar(String p_idOperario)
	{
		if (!estado.equals(DISPONIBLE) || bloqueada || riegoPendiente)
			return false;
		riegoPendiente = true;
		idOperarioRiego = p_idOperario;
		return true;
	}

	public synchronized void liberarReserva()
	{
		riegoPendiente = false;
		if (!estado.equals(REGANDO))
			idOperarioRiego = null;
	}

    // llega un dato de riego de la WS
	public synchronized boolean actualizarRiego(double p_caudal, double p_volumen, String p_idOperario)
	{
        // solo se acepta si la estacion esta disponible o regando
		if (!estado.equals(DISPONIBLE) && !estado.equals(REGANDO))
			return false;
		estado = REGANDO;
		riegoPendiente = false;
		caudalActual = p_caudal;
		volumenAcumulado = p_volumen;
		idOperarioRiego = p_idOperario;
		return true;
	}

	// termina el riego
	public synchronized void terminarRiego()
	{
		if (estado.equals(REGANDO))     // solo si está regando
			estado = DISPONIBLE;
		riegoPendiente = false;
		caudalActual = 0.0;
		volumenAcumulado = 0.0;
		idOperarioRiego = null;
	}

    public String getMotivoNoDisponible()
	{
		if (riegoPendiente)
			return "ya tiene un riego en arranque";
		switch (getEstadoVisible())
		{
			case REGANDO:        return "ya esta regando";
			case FUERA_SERVICIO: return "fuera de servicio";
			case FUGA:           return "averiada (fuga)";
			case DESCONECTADA:   return "desconectada";
			default:             return getEstadoVisible();
		}
	}
}