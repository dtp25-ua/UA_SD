import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;

// clase que gestiona la lista de estaciones de riego creadas mientras WM_Central está en ejecución
public class GestorEstaciones {
    
    // usamos ConcurrentHashMap para que varios hilos puedan acceder a la vez
    // hashMap: estructura <clave, valor>
    private ConcurrentHashMap<String, EstacionInfo> estaciones;

    // constructor
    public GestorEstaciones() 
    {
        this.estaciones = new ConcurrentHashMap<String, EstacionInfo>();
    }

    // usamos 'synchronized' para que solo un hilo pueda utilizar el método a la vez
    // si uno lo está usando y otro lo llama, este queda en espera
    public synchronized void registrarOActualizar(String p_id, String p_ubicacion, String p_estadoInicial)
    {
        // buscamos el id de la estacion a registrar/actualizar en la lista
        EstacionInfo estacion = estaciones.get(p_id);

        // si no encontramos el id, registramos la estacion
        if (estacion == null)
        {
            estacion = new EstacionInfo(p_id, p_ubicacion, p_estadoInicial);
            estaciones.put(p_id, estacion);
        }
        // si ya existe solo actualizamos sus atributos iniciales
        else 
        {
            estacion.setUbicacion(p_ubicacion);
            estacion.setEstado(p_estadoInicial);
        }
    }

    public EstacionInfo getEstacion(String p_id) { return estaciones.get(p_id); }
    public Collection<EstacionInfo> getAllEstaciones() { return estaciones.values(); }    
    public boolean existeEstacion(String p_id) { return estaciones.containsKey(p_id); }

    // metodo para imprimir el estado de todas las WS 
    public void imprimirPanel()
    {
        System.out.println("PANEL DE ESTACIONES");
        
        // recorremos todas las estaciones y llamamos a su metodo toString() 
        for (EstacionInfo e : getAllEstaciones())
        {
            System.out.println(e.toString());
        }    
    }
}
